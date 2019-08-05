target/release/lnchannels: src/main.rs src/bin/getdata.rs
	cargo build --release

deploy: target/release/lnchannels templates static channels.db
	rsync --progress target/release/lnchannels hutt:lnchannels/lnchannels
	rsync --progress -r templates hutt:lnchannels/
	rsync --progress -r static hutt:lnchannels/
	rsync --progress channels.db hutt:lnchannels/static/channels.db
	ssh hutt 'ln -sf ./static/channels.db lnchannels/channels.db'
