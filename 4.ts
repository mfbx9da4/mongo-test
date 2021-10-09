import { subYears } from 'date-fns'
import logUpdate from 'log-update'
import { generateQuery } from './generateQuery'
import { stocks } from './stocks'
import assert from 'assert'
import { sql } from '@leafac/sqlite'
import { getDb } from './sqlite'

console.log('stocks.length', stocks.length)

type Row = {
  /** stock_metric concatenated */
  key: string
  stock: string
  metric: string
  data: number[]
  /** timestamp start date */
  start: number
  /** timestamp end date */
  end: number
}

/**
 * For each ticker, generate ten values
 * Each one should be a float
 * For each year - 220 values for 15 years
 */
function* generateSeedData() {
  const metricsPerStock = 10
  const yearsPerStock = 15
  const pricesPerYear = 220
  const bufferSize = 10_000
  let rows: Row[] = []
  const value = Math.random()
  const end = Date.now()
  const start = subYears(end, yearsPerStock).valueOf()
  for (const stock of stocks) {
    for (let i = 0; i < metricsPerStock; i++) {
      const metric = String(i)
      const data: Row['data'] = []
      for (let j = 0; j < yearsPerStock; j++) {
        for (let k = 0; k < pricesPerYear; k++) {
          data.push(value)
        }
      }
      rows.push({ key: `${stock}_${metric}`, start, end, metric, stock, data })
      if (rows.length >= bufferSize) {
        yield rows
        rows = []
      }
    }
  }
  yield rows
}

function seedData() {
  const db = getDb()
  db.migrate(
    sql``,
    sql`
    CREATE TABLE "datum" ("key" VARCHAR(100), "stock" VARCHAR(50), "metric" VARCHAR(50), "start" NUMERIC, "end" NUMERIC, "data" TEXT);
    CREATE INDEX datum_index ON datum(key);
    `,
    sql`DROP TABLE datum`,
    sql`
    CREATE TABLE "datum" ("key" VARCHAR(100), "stock" VARCHAR(50), "metric" VARCHAR(50), "start" NUMERIC, "end" NUMERIC, "data" TEXT);
    CREATE INDEX datum_index ON datum(key);
    `
  )

  let count = 0
  for (const batch of generateSeedData()) {
    if (batch.length === 0) continue
    count += batch.length
    db.executeTransaction(() =>
      batch.map((x) => {
        db.run(
          sql`INSERT INTO "datum" ("key", "stock", "metric", "start", "end", "data") VALUES (${
            x.key
          }, ${x.stock}, ${x.metric}, ${x.start}, ${x.end}, ${JSON.stringify(
            x.data
          )})`
        )
      })
    )
    logUpdate(`inserted: ${count}`)
  }
}

function timeFindOne() {
  const db = getDb()
  console.time('findOne')
  const res = db.get<Row[]>(sql`SELECT * FROM "data" limit 1`)
  console.timeEnd('findOne')
  assert.ok(res, 'not found')
}

function timeQuery() {
  console.log('timeQuery')
  const db = getDb()
  const queryInput = generateQuery()
  const keys = queryInput.stocks.flatMap((x) =>
    queryInput.metrics.map((y) => `${x}_${y}`)
  )
  console.time('timeQuery')
  const stm = db.prepare(
    `SELECT * FROM datum where key in (${keys.map(() => '?').join(',')})`
  )
  const r = stm.all(...keys)
  console.timeEnd('timeQuery')
  console.log('returnedCount', r.length)
}

function main() {
  const db = getDb()
  const r = db.get(sql`SELECT count(*) FROM datum`)
  console.log('r', r)
  // seedData()
  // timeFindOne()
  timeQuery()

  return 'done.'
}

main()
process.on('exit', () => getDb().close())
process.on('SIGHUP', () => process.exit(128 + 1))
process.on('SIGINT', () => process.exit(128 + 2))
process.on('SIGTERM', () => process.exit(128 + 15))
