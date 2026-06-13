# demo.tape — vhs script that records the readstat CLI demo as an animated GIF.
#
# Renders scripts/demo.sh into docs/demo.gif. The .sh script is the source of
# truth for *what* runs; this tape controls *how it's recorded* (size, theme,
# pacing).
#
# Prerequisites (one-time, local):
#   brew install vhs          # pulls in ffmpeg + ttyd
#   cargo build --release -p readstat-cli --features sql
#
# Regenerate the GIF:
#   vhs docs/demo.tape        # run from the repo root
#
# Notes:
#   * demo.sh handles its own typewriter pacing via DEMO_SPEED, so we let it
#     drive the timing and just record the result here.
#   * DEMO_SQL=1 includes the SQL beat (needs the --features sql build above).

Output docs/demo.gif

Set Shell "bash"
Set FontSize 14
Set Width 1200
Set Height 800
Set Padding 20
Set Theme "Catppuccin Mocha"
Set TypingSpeed 0ms
Set PlaybackSpeed 1.0

# Run the demo. demo.sh prints the prompts and paces itself (DEMO_SPEED),
# so we hand the whole tour off to it in one command.
Type "DEMO_SPEED=1 DEMO_SQL=1 ./scripts/demo.sh"
Enter

# Give the run time to complete on screen before the recording ends.
# (~7 beats; bump this if you add beats or raise DEMO_SPEED.)
Sleep 30s
