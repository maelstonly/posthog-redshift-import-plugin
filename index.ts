import { Plugin, PluginEvent, PluginMeta } from '@posthog/plugin-scaffold'
import { Client, QueryResult, QueryResultRow } from 'pg'

declare namespace posthog {
    function capture(event: string, properties?: Record<string, any>): void
}

type RedshiftImportPlugin = Plugin<{
    global: {
        pgClient: Client
        eventsToIgnore: Set<string>
        sanitizedTableName: string
        initialOffset: number
        totalRows: number
    }
    config: {
        clusterHost: string
        clusterPort: string
        dbName: string
        tableName: string
        dbUsername: string
        dbPassword: string
        eventLogTableName: string
        pluginLogTableName: string
        eventsToIgnore: string
        orderByColumn: string
        transformationName: string
        importMechanism: 'Import continuously' | 'Only import historical data'
    }
}>

interface ImportEventsJobPayload extends Record<string, any> {
    offset?: number
    retriesPerformedSoFar: number
}

interface ExecuteQueryResponse {
    error: Error | null
    queryResult: QueryResult<any> | null
}

interface TransformedPluginEvent {
    event: string,
    properties?: PluginEvent['properties']
}

interface TransformationsMap {
    [key: string]: {
        author: string
        transform: (row: QueryResultRow, meta: PluginMeta<RedshiftImportPlugin>) => Promise<TransformedPluginEvent>
    }
}


const EVENTS_PER_BATCH = 10

const sanitizeSqlIdentifier = (unquotedIdentifier: string): string => {
    //console.log('sanitizeSqlIdentifier')
    //console.log(unquotedIdentifier)
    //console.log(unquotedIdentifier.replace(/[^\w\d_]+/g, ''))
    return unquotedIdentifier
    //return unquotedIdentifier.replace(/[^\w\d_]+/g, '')
}

const logMessage = async (message, config, logToRedshift = false) => {
    console.log('logMessage')
    console.log(message)
    if (logToRedshift) {
        const query = `INSERT INTO ${sanitizeSqlIdentifier(config.pluginLogTableName)} (event_at, message) VALUES (GETDATE(), $1)`
        console.log(query)
        const queryResponse = await executeQuery(query, [message], config)
        console.log(queryResponse)
    }
}

export const jobs: RedshiftImportPlugin['jobs'] = {
    importAndIngestEvents: async (payload, meta) => await importAndIngestEvents(payload as ImportEventsJobPayload, meta)
}

export const setupPlugin: RedshiftImportPlugin['setupPlugin'] = async ({ config, cache, jobs, global, storage }) => {
    await logMessage('setupPlugin', config, false)
    const requiredConfigOptions = ['clusterHost', 'clusterPort', 'dbName', 'dbUsername', 'dbPassword']
    for (const option of requiredConfigOptions) {
        if (!(option in config)) {
            throw new Error(`Required config option ${option} is missing!`)
        }
    }

    if (!config.clusterHost.endsWith('redshift.amazonaws.com')) {
        throw new Error('Cluster host must be a valid AWS Redshift host')
    }

    await jobs.importAndIngestEvents({ retriesPerformedSoFar: 0 }).runIn(5, 'seconds')
}


export const teardownPlugin: RedshiftImportPlugin['teardownPlugin'] = async ({ global, cache, storage }) => {
    return
}


const executeQuery = async (
    query: string,
    values: any[],
    config: PluginMeta<RedshiftImportPlugin>['config']
): Promise<ExecuteQueryResponse> => {
    console.log('executeQuery')
    const pgClient = new Client({
        user: config.dbUsername,
        password: config.dbPassword,
        host: config.clusterHost,
        database: config.dbName,
        port: parseInt(config.clusterPort),
    })

    await pgClient.connect()

    let error: Error | null = null
    let queryResult: QueryResult<any> | null = null
    try {
        queryResult = await pgClient.query(query, values)
    } catch (err) {
        error = err
        console.log(error)
    }

    await pgClient.end()

    return { error, queryResult }
}

const getTotalRowsToImport = async (config) => {
    const tableName = sanitizeSqlIdentifier(config.tableName),
          eventLogTableName = sanitizeSqlIdentifier(config.eventLogTableName)
    const totalRowsResult = await executeQuery(
        `SELECT COUNT(1) FROM ${tableName} WHERE NOT EXISTS (SELECT 1 FROM ${eventLogTableName} WHERE ${tableName}.event_id = ${eventLogTableName}.event_id)`,
        [],
        config
    )

    return Number(totalRowsResult.queryResult.rows[0].count)
}

const importAndIngestEvents = async (
    payload: ImportEventsJobPayload,
    meta: PluginMeta<RedshiftImportPlugin>
) => {
    console.log('importAndIngestEvents')
    if (payload.offset && payload.retriesPerformedSoFar >= 15) {
        console.error(`Import error: Unable to process rows ${payload.offset}-${
            payload.offset + EVENTS_PER_BATCH
        }. Skipped them.`)
        return
    }

    const { global, cache, config, jobs } = meta
    console.log('global', global)
    console.log('cache', cache)
    console.log('config', config)
    console.log('jobs', jobs)


    let offset: number
    if (payload.offset) {
        offset = payload.offset
    } else {
        const totalRowsToImport = await getTotalRowsToImport(config)
        console.log('getTotalRowsToImport results, ', totalRowsToImport)
    
        if (totalRowsToImport == 0) {
            console.log(`No rows to import in ${config.tableName}`)
            return
        }
        offset = 0
    }

    console.log(offset, global.totalRows)

    if (offset > global.totalRows) {
        console.log(`Done processing all rows in ${config.tableName}`)
        return
    }
    
    const query = `SELECT * FROM ${sanitizeSqlIdentifier(
        meta.config.tableName
    )} 
    ORDER BY ${sanitizeSqlIdentifier(config.orderByColumn)}
    OFFSET $1 LIMIT ${EVENTS_PER_BATCH}`

    const values = [offset]

    const queryResponse = await executeQuery(query, values, config)

    if (!queryResponse || queryResponse.error || !queryResponse.queryResult) {
        const nextRetrySeconds = 2 ** payload.retriesPerformedSoFar * 3
        console.log(
            `Unable to process rows ${offset}-${
                offset + EVENTS_PER_BATCH
            }. Retrying in ${nextRetrySeconds}. Error: ${queryResponse.error}`
        )
        await jobs
            .importAndIngestEvents({ ...payload, retriesPerformedSoFar: payload.retriesPerformedSoFar + 1 })
            .runIn(nextRetrySeconds, 'seconds')
    }

    const eventsToIngest: TransformedPluginEvent[] = []

    for (const row of queryResponse.queryResult!.rows) {
        const event = await transformations[config.transformationName].transform(row, meta)
        eventsToIngest.push(event)
    }

    const eventIdsIngested = []
    
    for (const event of eventsToIngest) {
        console.log(event)
        posthog.capture(event.event, event.properties)
        eventIdsIngested.push(event.id)
    }
    
    console.log(eventIdsIngested)
    
    const joinedEventIds = eventIdsIngested.map(x => `('${x}', GETDATE())`).join(',')
    
    const insertQuery = `INSERT INTO ${sanitizeSqlIdentifier(
        meta.config.eventLogTableName
    )} 
    (event_id, exported_at)
    VALUES
    ${joinedEventIds}`

    console.log(insertQuery)

    const insertQueryResponse = await executeQuery(insertQuery, [], config)

    console.log(insertQueryResponse)

    console.log(
        `Processed rows ${offset}-${offset + EVENTS_PER_BATCH} and ingested ${eventsToIngest.length} event${
            eventsToIngest.length > 1 ? 's' : ''
        } from them.`

    )
    await jobs.importAndIngestEvents({ retriesPerformedSoFar: 0 }).runNow()
}


// Transformations can be added by any contributor
// 'author' should be the contributor's GH username
const transformations: TransformationsMap = {
    'default': {
        author: 'yakkomajuri',
        transform: async (row, _) => {
            console.log('transforming')
            console.log(row)
            const { event_id, timestamp, distinct_id, event, properties, set } = row
            console.log(`timestamp = ${timestamp}, distinct_id=${distinct_id}, event=${event}, properties=${properties}, set=${set}`)
            const eventToIngest = { 
                "event": event, 
                id:event_id,
                properties: {
                    distinct_id, 
                    timestamp,
                    ...JSON.parse(properties), 
                    "$set": {
                        ...JSON.parse(set)
                    }
                }
            }
            console.log('eventToIngest')
            console.log(eventToIngest)
            console.log(`eventToIngest.event = ${eventToIngest.event}`)
            return eventToIngest
        }
    }
}
