import { readFile, writeFile } from 'fs/promises'
import { parse } from '@fast-csv/parse'
import { writeToString } from '@fast-csv/format'
import customParseFormat from 'dayjs/plugin/customParseFormat'
import dayjs from 'dayjs'

dayjs.extend(customParseFormat)

const config = {
  csvInputPath: 'data/input.csv',
  csvOutputPath: 'data/output.csv',
  lastIdPath: 'db/lastId.txt'
}

type RowParsed = {
  id: string
  date: string
  description: string
  transactionType: string
  amount: number
  currency: string
}

type RowYnab = {
  Date: string
  Payee: string
  Memo: string
  Outflow: string
  Inflow: string
}

const rowParsedToYnab = (row: RowParsed): RowYnab => ({
  Date: dayjs(row.date, 'DD/MM/YYYY').format('M/D/YY'),
  Payee: '',
  Memo: row.description,
  Outflow: String(row.amount * -1),
  Inflow: ''
})

const rates = {
  USD: (amount: number) => amount,
  GEL: (amount: number) => amount / 2.65
}

const parseCsvInput = async (lastId: string | null): Promise<RowParsed[]> => {
  const file = await readFile(config.csvInputPath, 'utf-8')
  const parsedRows: RowParsed[] = []

  return new Promise((resolve, reject) => {
    const stream = parse({ skipLines: 2 })
      .on('error', reject)
      .on('data', (row: string[]) => {
        const currency = row[4]
        if (Object.keys(rates).indexOf(currency) === -1) {
          console.warn(`Currency ${currency} is not supported`)
          return
        }
        const amountFloat = rates[currency as keyof typeof rates](parseInt(row[3]))
        const amount = Math.round(amountFloat * 100) / 100
        const formattedRow = {
          id: `${row[0]}-${row[1]}-${row[3]}}`,
          date: row[0],
          description: row[1],
          transactionType: row[2],
          amount,
          currency
        }
        if (formattedRow.transactionType !== 'Transfer out and cash withdrawal') {
          return
        }
        if (formattedRow.id === lastId) {
          stream.destroy()
          resolve(parsedRows)
          return
        }
        parsedRows.push(formattedRow)
      })
      .on('end', () => {
        resolve(parsedRows)
      })

    stream.write(file)
    stream.end()
  })
}

const getLastId = async () => {
  try {
    const file = await readFile(config.lastIdPath, 'utf-8')
    return file
  } catch (e) {
    console.warn('No lastId found')
    return null
  }
}

const setLastId = async (lastId: string) => {
  await writeFile(config.lastIdPath, lastId, 'utf-8')
}

const extractData = async () => {
  const write = process.argv.find((arg) => arg === '--write')
  const lastId = await getLastId()
  console.log('lastId', lastId)
  const parsedRows = await parseCsvInput(lastId)
  const nextLastId = parsedRows[0]?.id
  console.log('nextLastId', nextLastId)
  if (nextLastId && write) await setLastId(nextLastId)

  console.log(`Extracted ${parsedRows.length} rows`)
  console.log(
    'parsedRows',
    JSON.stringify(
      parsedRows.map((row) => row.id),
      null,
      2
    )
  )
  const rowsYnab = await writeToString(parsedRows.map(rowParsedToYnab), { headers: true })
  console.log('rowsYnab', rowsYnab)
  if (write) {
    await writeFile(config.csvOutputPath, rowsYnab, 'utf-8')
  }

  console.log(write ? 'Changes are written' : 'This is a preview, add --write to write changes')
  if (write) {
    console.log('lastId', lastId)
  }
}

extractData()
