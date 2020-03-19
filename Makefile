static/bundle.js: $(shell find ./client)
	./node_modules/.bin/rollup -c rollup.config.js

deploy: lnurl-tip
	ssh root@nusakan-58 'systemctl stop lnurl-tip'
	scp lnurl-tip nusakan-58:lnurl-tip/lnurl-tip
	ssh root@nusakan-58 'systemctl start lnurl-tip'
