# Records site/public/media/terminal-demo.gif from a real chaos run.
# Needs Go, VHS (https://github.com/charmbracelet/vhs) and a Postgres 17 you can
# create schemas in. The run uses its own schema and drops it when it passes.
#
#   DATABASE_URL='postgres://localhost:5432/durableq?sslmode=disable' vhs docs/demo/terminal-demo.tape
#
# Every line in the recording is real output; timings vary by machine.

Output site/public/media/terminal-demo.gif
Require go

Set Shell "bash"
Set FontSize 15
Set Width 1200
Set Height 640
Set Padding 24
Set Framerate 12
Set TypingSpeed 30ms
Set Theme "Builtin Dark"

Hide
Type 'export BIN=$(mktemp -d) && go build -o $BIN/durableq ./cmd/durableq && export PATH=$BIN:$PATH PS1="$ " && clear'
Enter
Sleep 10s
Show

Type "# 3,000 jobs, 4 worker processes; SIGKILL workers, kill their DB connections, freeze them past their leases"
Enter
Sleep 600ms
Type "durableq demo --jobs 3000 --workers 4 --kills 12 --conn-kills 3 --stalls 2 --restarts 3 --seed 7"
Sleep 400ms
Enter
Sleep 32s
Sleep 5s
