import assert from 'assert'
import faker from 'faker'
import logUpdate from 'log-update'
import { Db, MongoClient } from 'mongodb'
import { stocks as allStocks } from './stocks'
const url = process.env.MONGO_URL || `mongodb://root:example@localhost:27017/`
const client = new MongoClient(url)
const DB_NAME = 'myDbName'

function randomInt(a) {
  return Math.floor(Math.random() * a)
}

const stocks = allStocks.slice(0, 500)
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
  const yearsPerStock = 3
  const pricesPerYear = 220
  const bufferSize = 10_000
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

async function seedData() {
  const db = await getDb()
  await db.dropDatabase()
  const collectionA = db.collection('a')
  const collectionB = db.collection('b')
  let count = 0
  for (const batch of generateSeedData()) {
    if (batch.length === 0) continue
    count += batch.length
    await collectionA.insertMany(batch)
    logUpdate(`inserted: ${count}`)
  }
  await collectionA.createIndex([
    ['stock', 1],
    ['metric', 1],
    ['date', 1],
  ])
  console.log('created index')
}

async function timeFindOne() {
  const collection = await getCollection('a')
  console.time('findOne')
  const res = await collection.find({}, { limit: 1 }).toArray()
  console.timeEnd('findOne')
  assert.ok(res.length === 1, 'not found')
  const explain = await collection.find({}, { limit: 1 }).explain()
  console.log(explain.queryPlanner)
}

function generateQuery() {
  const stocksQ = Array.from(
    { length: 500 },
    () => stocks[randomInt(stocks.length)]
  )
  const metrics = Array.from({ length: 4 }, () => randomInt(10))
  const dateRange = {
    start: faker.date.between(
      '2000/01/01',
      new Date(Date.now() - 1000 * 60 * 60 * 24 * 365 * 2)
    ),
    end: new Date(),
  }

  return { stocks: stocksQ, metrics, dateRange }
}

async function timeQuery1() {
  console.log('timeQuery1')
  const collection = await getCollection('a')
  const queryInput = generateQuery()
  console.log(queryInput)
  console.time('findData')
  const r = await collection
    .find({
      stock: { $in: queryInput.stocks },
      metric: { $in: queryInput.metrics },
      date: { $gt: queryInput.dateRange.start },
    })
    .toArray()
  console.timeEnd('findData')
  console.log('returnedCount', r.length)
  const explain = await collection
    .find({
      stock: { $in: queryInput.stocks },
      metric: { $in: queryInput.metrics },
      date: { $gt: queryInput.dateRange.start },
    })
    .explain()
  console.log('explain', explain.queryPlanner)
}

let db: Db | undefined
async function getDb() {
  if (db) return db
  console.log('connecting')
  await client.connect()
  console.log('connected')
  db = client.db(DB_NAME)
  return db
}

async function getCollection(name) {
  const db = await getDb()
  return db.collection(name)
}

async function main() {
  // const collection = await getCollection('a')
  // console.log('collection.count()', await collection.estimatedDocumentCount())
  // await seedData()
  await timeFindOne()
  // await timeQuery1()

  return 'done.'
}

main()
  .then(console.log)
  .catch(console.error)
  .finally(() => client.close())
