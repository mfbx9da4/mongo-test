import { subYears } from 'date-fns'
import logUpdate from 'log-update'
import { generateQuery } from './generateQuery'
import { getClient, getCollection, getDb } from './mongo'
import { stocks } from './stocks'
import assert from 'assert'

console.log('stocks.length', stocks.length)

type Row = {
  key: `${Row['stock']}_${Row['metric']}`
  stock: string
  metric: string
  data: number[]
  start: number
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

async function seedData() {
  const db = await getDb()
  await db.dropDatabase()
  const collection = db.collection('b')
  let count = 0
  for (const batch of generateSeedData()) {
    if (batch.length === 0) continue
    count += batch.length
    await collection.insertMany(batch)
    logUpdate(`inserted: ${count}`)
  }
  await collection.createIndex([
    ['key', 1],
    ['start', 1],
    ['end', 1],
  ])
  console.log('created index')
}

async function timeFindOne() {
  const collection = await getCollection('b')
  console.time('findOne')
  const res = await collection.find({}, { limit: 1 }).toArray()
  console.timeEnd('findOne')
  assert.ok(res.length === 1, 'not found')
  const explain = await collection.find({}, { limit: 1 }).explain()
  console.log(explain.queryPlanner)
  console.log('res[0]', res[0])
}

async function timeQuery() {
  console.log('timeQuery')
  const collection = await getCollection('b')
  const queryInput = generateQuery()
  const keys = queryInput.stocks.flatMap((x) =>
    queryInput.metrics.map((y) => `${x}_${y}`)
  )
  console.time('findData')
  const query = {
    key: { $in: keys },
    // date: { $gt: queryInput.dateRange.start },
  }
  const r = await collection.find(query).toArray()
  console.timeEnd('findData')
  console.log('returnedCount', r.length)
  const explain = await collection.find(query).explain()
  console.log('explain', explain.queryPlanner)
}

async function main() {
  const collection = await getCollection('b')
  console.log('collection.count()', await collection.estimatedDocumentCount())
  // await seedData()
  // await timeFindOne()
  await timeQuery()

  return 'done.'
}

const client = getClient()

main()
  .then(console.log)
  .catch(console.error)
  .finally(() => client.close())
