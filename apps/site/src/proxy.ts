import { clerkMiddleware } from "@clerk/nextjs/server";
import { isAppGated } from "@/lib/gates";

export default clerkMiddleware(async (auth) => {
  // Preview and local environments intentionally remain public.
  if (process.env.VERCEL_ENV !== "production") {
    return;
  }

  if (await isAppGated()) {
    await auth.protect();
  }
});

export const config = {
  matcher: [
    /*
     * Match all request paths except for static files and Next.js internals.
     * This avoids unnecessary Clerk work for immutable public assets.
     */
    "/((?!_next|[^?]*\\.(?:html?|css|js(?!on)|json|jpe?g|png|gif|svg|webp|ico|ttf|woff2?|map)).*)",
    "/(api|trpc)(.*)",
  ],
};
