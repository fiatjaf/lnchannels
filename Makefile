target/release/server: src/bin/server.rs src/bin/getdata.rs
	cargo build --release

deploy: target/release/server templates static channels.db
	rsync --progress target/release/server nusakan-58:lnchannels/lnchannels
	rsync --progress -r templates nusakan-58:lnchannels/
	rsync --progress -r static nusakan-58:lnchannels/
	rsync --progress channels.db nusakan-58:lnchannels/static/channels.db
	ssh nusakan-58 'ln -sf ./static/channels.db lnchannels/channels.db'
