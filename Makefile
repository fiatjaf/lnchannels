static/bundle.js: $(shell find ./client)
	./node_modules/.bin/rollup -c rollup.config.js

deploy: static/bundle.js
	rsync -r static fuyue-421:lnchannels

dump:
	godotenv bash -c 'ssh fuyue-421 "pg_dump $$POSTGRES_URL > lnchannels/static/lnchannels.dump"'
	godotenv bash -c 'ssh fuyue-421 "echo \"COPY (SELECT * FROM channels) TO STDOUT WITH CSV HEADER\" | psql $$POSTGRES_URL > lnchannels/static/channels.csv"'

getdata:
	godotenv python -m getdata

routine: backup getdata

.PHONY: getdata dump
