target/release/server: src/bin/server.rs src/bin/getdata.rs
	cargo build --release

deploy: target/release/server templates static channels.db
	rsync --progress target/release/server hutt:lnchannels/lnchannels
	rsync --progress -r templates hutt:lnchannels/
	rsync --progress -r static hutt:lnchannels/
	rsync --progress channels.db hutt:lnchannels/static/channels.db
	ssh hutt 'ln -sf ./static/channels.db lnchannels/channels.db'
