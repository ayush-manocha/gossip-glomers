MAELSTROM = ../maelstrom/maelstrom

echo:
	go install ./maelstrom-echo
	$(MAELSTROM) test -w echo --bin ~/go/bin/maelstrom-echo --node-count 1 --time-limit 10
