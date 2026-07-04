MAELSTROM = ../maelstrom/maelstrom

echo:
	go install ./maelstrom-echo
	$(MAELSTROM) test -w echo --bin ~/go/bin/maelstrom-echo --node-count 1 --time-limit 10

unique-ids:
	go install ./maelstrom-unique-ids
	$(MAELSTROM) test -w unique-ids --bin ~/go/bin/maelstrom-unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
