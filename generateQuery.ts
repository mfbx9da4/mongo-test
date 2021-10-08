import faker from 'faker'
import { randomInt } from './randomInt'
import { stocks } from './stocks'

export function generateQuery(opts?: { stocksN?: number; metricsN?: number }) {
  const safeOpts = { stocksN: 500, metricsN: 4, ...opts }
  const stocksQ = Array.from(
    { length: safeOpts.stocksN },
    () => stocks[randomInt(stocks.length)]
  )
  const metrics = Array.from({ length: safeOpts.metricsN }, () => randomInt(10))
  const dateRange = {
    start: faker.date.between(
      '2000/01/01',
      new Date(Date.now() - 1000 * 60 * 60 * 24 * 365 * 2)
    ),
    end: new Date(),
  }

  return { stocks: stocksQ, metrics, dateRange }
}
