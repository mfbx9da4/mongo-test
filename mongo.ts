import { Db } from 'mongodb'
import { MongoClient } from 'mongodb'

const DB_NAME = 'myDbName'

let client: MongoClient | undefined
export function getClient() {
  if (client) return client
  const url = process.env.MONGO_URL || `mongodb://root:example@localhost:27017/`
  client = new MongoClient(url)
  return client
}

let db: Db | undefined
export async function getDb() {
  const client = getClient()
  if (db) return db
  console.log('connecting')
  await client.connect()
  console.log('connected')
  db = client.db(DB_NAME)
  return db
}

export async function getCollection(name) {
  const db = await getDb()
  return db.collection(name)
}
