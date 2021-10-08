import { sql } from '@leafac/sqlite'
import assert from 'assert'
import faker from 'faker'
import logUpdate from 'log-update'
import { generateQuery } from './generateQuery'
import { getDb } from './sqlite'

import { stocks } from './stocks'

console.log('stocks.length', stocks.length)

type Row = {
  stock: string
  metric: string
  value: number
  date: Date
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
  const bufferSize = 100_000
  let byValue: Row[] = []
  const value = Math.random()
  const now = new Date()
  for (const stock of stocks) {
    for (let i = 0; i < metricsPerStock; i++) {
      for (let j = 0; j < yearsPerStock; j++) {
        for (let k = 0; k < pricesPerYear; k++) {
          const x = {
            stock,
            metric: String(i),
            value,
            date: faker.date.between('2000-01-01', now),
          }
          byValue.push(x)
          if (byValue.length >= bufferSize) {
            yield byValue
            byValue = []
          }
        }
      }
    }
  }
  yield byValue
}

function seedData() {
  const db = getDb()
  db.migrate(sql`
    CREATE TABLE "data" ("stock" VARCHAR(50), "metric" VARCHAR(50), "value" NUMERIC, "date" NUMERIC);
    CREATE INDEX search_index ON data(stock, metric, date);
    `)

  let count = 0
  for (const batch of generateSeedData()) {
    if (batch.length === 0) continue
    count += batch.length
    db.executeTransaction(() =>
      batch.map((x) => {
        db.run(
          sql`INSERT INTO data ("stock", "metric", "value", "date") VALUES (${
            x.stock
          }, ${x.metric}, ${x.value}, ${x.date.valueOf()})`
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
  console.log(queryInput)
  const foo = ['CPTA', 'CIBR', 'BANFP']
  console.time('findData')
  const a = db.all(sql`SELECT * FROM data where stock in ${foo} LIMIT 1`)
  console.log('a', a)
  return
  const stm = db.prepare(`SELECT * FROM data where 
  stock in (${queryInput.stocks.map(() => '?').join(',')}) and
  metric in (${queryInput.metrics.map(() => '?').join(',')}) and
  date > ?
  `)
  const r = stm.all(
    ...[queryInput.stocks],
    ...[queryInput.metrics],
    queryInput.dateRange.start.valueOf()
  )
  console.timeEnd('findData')
  console.log('returnedCount', r.length)
}

function main() {
  const db = getDb()
  console.log(db.pragma('user_version'))
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
