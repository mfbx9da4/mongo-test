# Mongo tests

Testing how to index and query data best in mongo.
[More details about the specific problem can be found here.](https://davidadler.notion.site/Stratiphy-query-4d7d0d962568441ab7dcbaade81816f6)

## Run locally

Pre-requisites:

- node14 or higher
- npm7 or higher
- docker

**Install**

```sh
npm install
docker-compose up -d # starts mongo
```

**Run**

```sh
node --require tsm test.ts
```
