use crate::{
    AppendEntriesReq, AppendEntriesResp, InstallSnapshotReq, InstallSnapshotResp, LogEntry,
    Message, RequestVoteReq, RequestVoteResp,
};
use std::io::ErrorKind;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const DECODE_PROGRESS_CHUNK: usize = 64 * 1024;

struct FrameReader<'a, R> {
    reader: &'a mut R,
    remaining: usize,
    idle_timeout: Duration,
}

pub(super) fn serialized_frame_len(message: &Message) -> crate::Result<u32> {
    let payload_len = bincode::serialized_size(message).map_err(|error| {
        crate::Error::Corruption(format!("message size calculation failed: {error}"))
    })?;
    u32::try_from(payload_len).map_err(|_| {
        crate::Error::Corruption("message payload does not fit in a u32 frame length".to_string())
    })
}

pub(super) async fn write_frame<W>(
    writer: &mut W,
    message: &Message,
    payload_len: u32,
    idle_timeout: Duration,
) -> crate::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_all_with_idle_timeout(writer, &payload_len.to_le_bytes(), idle_timeout).await?;
    write_message(writer, message, idle_timeout).await
}

pub(super) async fn read_message<R>(
    reader: &mut R,
    payload_len: usize,
    idle_timeout: Duration,
) -> crate::Result<Message>
where
    R: AsyncRead + Unpin,
{
    let mut frame = FrameReader {
        reader,
        remaining: payload_len,
        idle_timeout,
    };
    let variant = frame.read_u32().await?;
    let message = match variant {
        0 => Message::RequestVote(RequestVoteReq {
            term: frame.read_u64().await?,
            candidate_id: frame.read_u64().await?,
            last_log_index: frame.read_u64().await?,
            last_log_term: frame.read_u64().await?,
            pre_vote: frame.read_bool().await?,
        }),
        1 => Message::RequestVoteResp(RequestVoteResp {
            term: frame.read_u64().await?,
            vote_granted: frame.read_bool().await?,
        }),
        2 => {
            let term = frame.read_u64().await?;
            let leader_id = frame.read_u64().await?;
            let prev_log_index = frame.read_u64().await?;
            let prev_log_term = frame.read_u64().await?;
            let entries = frame.read_log_entries().await?;
            let leader_commit = frame.read_u64().await?;
            Message::AppendEntries(AppendEntriesReq {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            })
        }
        3 => Message::AppendEntriesResp(AppendEntriesResp {
            term: frame.read_u64().await?,
            success: frame.read_bool().await?,
            conflict_index: frame.read_optional_u64().await?,
        }),
        4 => Message::InstallSnapshot(InstallSnapshotReq {
            term: frame.read_u64().await?,
            leader_id: frame.read_u64().await?,
            last_index: frame.read_u64().await?,
            last_term: frame.read_u64().await?,
            data: frame.read_bytes().await?,
        }),
        5 => Message::InstallSnapshotResp(InstallSnapshotResp {
            term: frame.read_u64().await?,
        }),
        invalid => {
            return Err(corruption(format!(
                "invalid bincode Message variant {invalid}"
            )));
        }
    };
    if frame.remaining != 0 {
        return Err(corruption(format!(
            "message deserialization left {} trailing payload bytes",
            frame.remaining
        )));
    }
    Ok(message)
}

pub(super) async fn read_exact_with_idle_timeout<R>(
    reader: &mut R,
    mut buffer: &mut [u8],
    idle_timeout: Duration,
) -> crate::Result<()>
where
    R: AsyncRead + Unpin,
{
    while !buffer.is_empty() {
        let bytes_read = tokio::time::timeout(idle_timeout, reader.read(buffer))
            .await
            .map_err(|_| {
                io_error(
                    ErrorKind::TimedOut,
                    "TCP inbound read made no progress before idle deadline",
                )
            })??;
        if bytes_read == 0 {
            return Err(io_error(
                ErrorKind::UnexpectedEof,
                "TCP inbound stream ended before the frame completed",
            ));
        }
        let (_, remaining) = buffer.split_at_mut(bytes_read);
        buffer = remaining;
    }
    Ok(())
}

pub(super) async fn write_all_with_idle_timeout<W>(
    writer: &mut W,
    mut buffer: &[u8],
    idle_timeout: Duration,
) -> crate::Result<()>
where
    W: AsyncWrite + Unpin,
{
    while !buffer.is_empty() {
        let bytes_written = tokio::time::timeout(idle_timeout, writer.write(buffer))
            .await
            .map_err(|_| {
                io_error(
                    ErrorKind::TimedOut,
                    "TCP outbound write made no progress before idle deadline",
                )
            })??;
        if bytes_written == 0 {
            return Err(io_error(
                ErrorKind::WriteZero,
                "TCP outbound stream stopped accepting frame bytes",
            ));
        }
        buffer = &buffer[bytes_written..];
    }
    Ok(())
}

pub(super) fn allocate_bytes(length: usize) -> crate::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length).map_err(|error| {
        io_error(
            ErrorKind::OutOfMemory,
            format!("cannot allocate incoming message bytes: {error}"),
        )
    })?;
    Ok(bytes)
}

async fn write_message<W>(
    writer: &mut W,
    message: &Message,
    idle_timeout: Duration,
) -> crate::Result<()>
where
    W: AsyncWrite + Unpin,
{
    match message {
        Message::RequestVote(request) => {
            write_u32(writer, 0, idle_timeout).await?;
            write_u64(writer, request.term, idle_timeout).await?;
            write_u64(writer, request.candidate_id, idle_timeout).await?;
            write_u64(writer, request.last_log_index, idle_timeout).await?;
            write_u64(writer, request.last_log_term, idle_timeout).await?;
            write_bool(writer, request.pre_vote, idle_timeout).await
        }
        Message::RequestVoteResp(response) => {
            write_u32(writer, 1, idle_timeout).await?;
            write_u64(writer, response.term, idle_timeout).await?;
            write_bool(writer, response.vote_granted, idle_timeout).await
        }
        Message::AppendEntries(request) => {
            write_u32(writer, 2, idle_timeout).await?;
            write_u64(writer, request.term, idle_timeout).await?;
            write_u64(writer, request.leader_id, idle_timeout).await?;
            write_u64(writer, request.prev_log_index, idle_timeout).await?;
            write_u64(writer, request.prev_log_term, idle_timeout).await?;
            write_sequence_len(writer, request.entries.len(), idle_timeout).await?;
            for entry in &request.entries {
                write_log_entry(writer, entry, idle_timeout).await?;
            }
            write_u64(writer, request.leader_commit, idle_timeout).await
        }
        Message::AppendEntriesResp(response) => {
            write_u32(writer, 3, idle_timeout).await?;
            write_u64(writer, response.term, idle_timeout).await?;
            write_bool(writer, response.success, idle_timeout).await?;
            match response.conflict_index {
                Some(index) => {
                    write_u8(writer, 1, idle_timeout).await?;
                    write_u64(writer, index, idle_timeout).await
                }
                None => write_u8(writer, 0, idle_timeout).await,
            }
        }
        Message::InstallSnapshot(request) => {
            write_u32(writer, 4, idle_timeout).await?;
            write_u64(writer, request.term, idle_timeout).await?;
            write_u64(writer, request.leader_id, idle_timeout).await?;
            write_u64(writer, request.last_index, idle_timeout).await?;
            write_u64(writer, request.last_term, idle_timeout).await?;
            write_bytes(writer, &request.data, idle_timeout).await
        }
        Message::InstallSnapshotResp(response) => {
            write_u32(writer, 5, idle_timeout).await?;
            write_u64(writer, response.term, idle_timeout).await
        }
    }
}

async fn write_log_entry<W>(
    writer: &mut W,
    entry: &LogEntry,
    idle_timeout: Duration,
) -> crate::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_u64(writer, entry.term, idle_timeout).await?;
    write_u64(writer, entry.index, idle_timeout).await?;
    write_bytes(writer, &entry.command, idle_timeout).await
}

async fn write_bytes<W>(writer: &mut W, bytes: &[u8], idle_timeout: Duration) -> crate::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_sequence_len(writer, bytes.len(), idle_timeout).await?;
    write_all_with_idle_timeout(writer, bytes, idle_timeout).await
}

async fn write_sequence_len<W>(
    writer: &mut W,
    length: usize,
    idle_timeout: Duration,
) -> crate::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let length = u64::try_from(length)
        .map_err(|_| corruption("message sequence length does not fit in bincode u64"))?;
    write_u64(writer, length, idle_timeout).await
}

async fn write_bool<W>(writer: &mut W, value: bool, idle_timeout: Duration) -> crate::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_u8(writer, u8::from(value), idle_timeout).await
}

async fn write_u8<W>(writer: &mut W, value: u8, idle_timeout: Duration) -> crate::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_all_with_idle_timeout(writer, &[value], idle_timeout).await
}

async fn write_u32<W>(writer: &mut W, value: u32, idle_timeout: Duration) -> crate::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_all_with_idle_timeout(writer, &value.to_le_bytes(), idle_timeout).await
}

async fn write_u64<W>(writer: &mut W, value: u64, idle_timeout: Duration) -> crate::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_all_with_idle_timeout(writer, &value.to_le_bytes(), idle_timeout).await
}

impl<R> FrameReader<'_, R>
where
    R: AsyncRead + Unpin,
{
    async fn read_exact(&mut self, buffer: &mut [u8]) -> crate::Result<()> {
        if buffer.len() > self.remaining {
            return Err(corruption(format!(
                "message field requires {} bytes but only {} frame bytes remain",
                buffer.len(),
                self.remaining
            )));
        }
        read_exact_with_idle_timeout(self.reader, buffer, self.idle_timeout).await?;
        self.remaining -= buffer.len();
        Ok(())
    }

    async fn read_u8(&mut self) -> crate::Result<u8> {
        let mut bytes = [0; 1];
        self.read_exact(&mut bytes).await?;
        Ok(bytes[0])
    }

    async fn read_u32(&mut self) -> crate::Result<u32> {
        let mut bytes = [0; 4];
        self.read_exact(&mut bytes).await?;
        Ok(u32::from_le_bytes(bytes))
    }

    async fn read_u64(&mut self) -> crate::Result<u64> {
        let mut bytes = [0; 8];
        self.read_exact(&mut bytes).await?;
        Ok(u64::from_le_bytes(bytes))
    }

    async fn read_bool(&mut self) -> crate::Result<bool> {
        match self.read_u8().await? {
            0 => Ok(false),
            1 => Ok(true),
            invalid => Err(corruption(format!(
                "invalid bincode boolean encoding {invalid}"
            ))),
        }
    }

    async fn read_optional_u64(&mut self) -> crate::Result<Option<u64>> {
        match self.read_u8().await? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u64().await?)),
            invalid => Err(corruption(format!(
                "invalid bincode option encoding {invalid}"
            ))),
        }
    }

    async fn read_bytes(&mut self) -> crate::Result<Vec<u8>> {
        let length = self.read_u64().await?;
        if length > self.remaining as u64 {
            return Err(corruption(format!(
                "message byte sequence length {length} exceeds {} remaining frame bytes",
                self.remaining
            )));
        }
        let length = usize::try_from(length)
            .map_err(|_| corruption("message byte sequence length does not fit in usize"))?;
        let mut bytes = allocate_bytes(length)?;
        while bytes.len() != length {
            let chunk_start = bytes.len();
            let chunk_end = chunk_start
                .saturating_add(DECODE_PROGRESS_CHUNK)
                .min(length);
            bytes.resize(chunk_end, 0);
            self.read_exact(&mut bytes[chunk_start..]).await?;
        }
        Ok(bytes)
    }

    async fn read_log_entries(&mut self) -> crate::Result<Vec<LogEntry>> {
        const MIN_LOG_ENTRY_BYTES: usize = 24;
        const TRAILING_LEADER_COMMIT_BYTES: usize = 8;

        let count = self.read_u64().await?;
        let available_entry_bytes = self
            .remaining
            .checked_sub(TRAILING_LEADER_COMMIT_BYTES)
            .ok_or_else(|| corruption("append entries frame is missing leader_commit"))?;
        if count > (available_entry_bytes / MIN_LOG_ENTRY_BYTES) as u64 {
            return Err(corruption(format!(
                "append entries count {count} exceeds the remaining frame boundary"
            )));
        }
        let count = usize::try_from(count)
            .map_err(|_| corruption("append entries count does not fit in usize"))?;
        let mut entries = Vec::new();
        entries.try_reserve_exact(count).map_err(|error| {
            io_error(
                ErrorKind::OutOfMemory,
                format!("cannot allocate incoming log entries: {error}"),
            )
        })?;
        for _ in 0..count {
            entries.push(LogEntry {
                term: self.read_u64().await?,
                index: self.read_u64().await?,
                command: self.read_bytes().await?,
            });
        }
        Ok(entries)
    }
}

fn corruption(message: impl Into<String>) -> crate::Error {
    crate::Error::Corruption(message.into())
}

fn io_error(kind: ErrorKind, message: impl Into<String>) -> crate::Error {
    std::io::Error::new(kind, message.into()).into()
}
