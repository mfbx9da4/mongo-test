import { Database, sql } from '@leafac/sqlite'

let db: Database | undefined
export function getDb() {
  if (db) return db
  db = new Database('foobar.sqlite')
  return db
}
