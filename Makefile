static/bundle.js: $(shell find ./client)
	./node_modules/.bin/rollup -c rollup.config.js

deploy: static/bundle.js
	rsync -r static nusakan-58:lnchannels

dump:
	godotenv bash -c 'ssh nusakan-58 "pg_dump $$POSTGRES_URL > lnchannels/static/lnchannels.dump"'

getdata:
	godotenv python -m getdata

routine: backup getdata

.PHONY: getdata dump
