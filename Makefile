static/bundle.js: $(shell find ./client)
	./node_modules/.bin/rollup -c rollup.config.js

deploy: static/bundle.js
	rsync -r static hulsmann:lnchannels

dump:
	godotenv bash -c 'ssh hulsmann "pg_dump $$POSTGRES_URL > lnchannels/static/lnchannels.dump"'
	godotenv bash -c 'ssh hulsmann "echo \"COPY (SELECT * FROM channels) TO STDOUT WITH CSV HEADER\" | psql $$POSTGRES_URL > lnchannels/static/channels.csv"'

getdata:
	godotenv python -m getdata

routine: backup getdata

.PHONY: getdata dump
