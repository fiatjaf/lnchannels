static/bundle.js: $(shell find ./client)
	./node_modules/.bin/rollup -c rollup.config.js

deploy: static/bundle.js
	rsync -r static nusakan-58:lnchannels

dump:
	godotenv fish -c 'pg_dump $$POSTGRES_URL >> lnchannels.dump'
	rsync -r lnchannels.dump nusakan-58:lnchannels/static/lnchannels.dump

getdata:
	godotenv python -m getdata

routine: backup getdata

.PHONY: getdata dump
