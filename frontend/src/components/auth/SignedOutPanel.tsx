import { SignInButton } from "@/auth/clerk";

import { BackgroundPaths } from "@/components/ui/background-paths";
import { Button } from "@/components/ui/button";

type SignedOutPanelProps = {
  message: string;
  forceRedirectUrl: string;
  signUpForceRedirectUrl?: string;
  mode?: "modal" | "redirect";
  buttonLabel?: string;
  buttonTestId?: string;
};

export function SignedOutPanel({
  message,
  forceRedirectUrl,
  signUpForceRedirectUrl,
  mode = "modal",
  buttonLabel = "Sign in",
  buttonTestId,
}: SignedOutPanelProps) {
  return (
    <div className="col-span-2 flex min-h-[calc(100vh-64px)] items-center justify-center p-4 text-center sm:p-8">
      <div className="w-full max-w-5xl">
        <BackgroundPaths
          eyebrow="Mission Control"
          title="Unlock your operations cockpit"
          description={message}
          className="min-h-[420px] p-0"
        />
        <div className="-mt-36 flex justify-center px-4 pb-4 sm:-mt-28">
          <div className="w-full max-w-md rounded-[1.8rem] border border-white/60 bg-white/88 px-6 py-6 shadow-[0_28px_70px_rgba(8,145,178,0.14)] backdrop-blur-xl sm:px-8">
            <p className="text-sm leading-6 text-slate-600">{message}</p>
            <SignInButton
              mode={mode}
              forceRedirectUrl={forceRedirectUrl}
              signUpForceRedirectUrl={signUpForceRedirectUrl}
            >
              <Button className="mt-5 w-full" data-testid={buttonTestId}>
                {buttonLabel}
              </Button>
            </SignInButton>
          </div>
        </div>
      </div>
    </div>
  );
}
