import { Plugin, PluginEvent, PluginMeta } from '@posthog/plugin-scaffold'
import { Client, QueryResult, QueryResultRow } from 'pg'

declare namespace posthog {
    function capture(event: string, properties?: Record<string, any>): void
}
type RedshiftImportPlugin = Plugin<{
    global: {
        pgClient: Client
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
        orderByColumn: string
        eventLogTableName: string
        eventLogFailedTableName: string
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
const EVENTS_PER_BATCH = 200
const RUN_LIMIT = 20
const WHEN_DONE_NEXT_JOB_SCHEDULE_SECONDS = 2
const IS_CURRENTLY_IMPORTING = 'new_key_21'
const TRANSFORMATION_NAME = 'users_group'
const sanitizeSqlIdentifier = (unquotedIdentifier: string): string => {
    return unquotedIdentifier
}
const logMessage = async (message, config, logToRedshift = false) => {
    console.log(message)
    if (logToRedshift) {
        const query = `INSERT INTO ${sanitizeSqlIdentifier(config.pluginLogTableName)} (event_at, message) VALUES (GETDATE(), $1)`
        const queryResponse = await executeQuery(query, [message], config)
    }
}

export const jobs: RedshiftImportPlugin['jobs'] = {
    importAndIngestEvent: async (payload, meta) => await importAndIngestEvent(payload as ImportEventsJobPayload, meta)
}


export const setupPlugin: RedshiftImportPlugin['setupPlugin'] = async ({ config, cache, jobs, global, storage, utils}) => {
    console.log('setupPlugin')
    const requiredConfigOptions = ['clusterHost', 'clusterPort', 'dbName', 'dbUsername', 'dbPassword']
    for (const option of requiredConfigOptions) {
        if (!(option in config)) {
            throw new Error(`Required config option ${option} is missing!`)
        }
    }
    if (!config.clusterHost.endsWith('redshift.amazonaws.com')) {
        throw new Error('Cluster host must be a valid AWS Redshift host')
    }

    const cursor = utils.cursor
    
    await cursor.init(IS_CURRENTLY_IMPORTING)
    console.log('cursor (1):', IS_CURRENTLY_IMPORTING)
    
    const cursorValue = await cursor.increment(IS_CURRENTLY_IMPORTING)
    console.log('cursor (2):', cursorValue)
    
    if (cursorValue > 1) {
        console.log('EXIT due to cursorValue > 1')
        return
    }
    
    console.log('launching job')
    const totalRowsToImport = await getTotalRowsToImport(config)
    const jobs.importAndIngestEvent({ retriesPerformedSoFar: 0, successiveRuns: 0 }).runIn(10, 'seconds')
    console.log('job finished')
}

//EXECUTE QUERY FUNCTION
const executeQuery = async (
    query: string,
    values: any[],
    config: PluginMeta<RedshiftImportPlugin>['config']
): Promise<ExecuteQueryResponse> => {
    const pgClient = new Client({
        user: config.dbUsername,
        password: config.dbPassword,
        host: config.clusterHost,
        database: config.dbName,
        port: parseInt(config.clusterPort),
    })
    let startedAt : Date =  new Date()

    await pgClient.connect()
    let error: Error | null = null
    let queryResult: QueryResult<any> | null = null
    try {
        queryResult = await pgClient.query(query, values)
    } catch {
        try {
            queryResult = await pgClient.query(query, values)
        } catch (err) {
            error = err
        }
    }
    await pgClient.end()

    let finishedAt : Date =  new Date()
    let queryDurationSeconds : number = finishedAt - startedAt
    console.log('query result', query.split(' ')[0], 'duration (seconds) =', queryDurationSeconds)

    return { error, queryResult }
}

const getTotalRowsToImport = async (config) => {
    const totalRowsQuery = `SELECT COUNT(1) FROM ${sanitizeSqlIdentifier(config.tableName)} 
         WHERE NOT EXISTS (
             SELECT 1 FROM ${sanitizeSqlIdentifier(config.eventLogTableName)} 
             WHERE ${sanitizeSqlIdentifier(config.tableName)}.event_id = ${sanitizeSqlIdentifier(config.eventLogTableName)}.event_id
             )`
    
    console.log('first step of total rows')
    
    const totalRowsResult = await executeQuery(totalRowsQuery, [], config)
    
    if (!totalRowsResult || totalRowsResult.error || !totalRowsResult.queryResult) {
        console.log(totalRowsQuery)
        console.log(totalRowsResult)
        throw new Error(`Error while getting total rows to import: ${totalRowsResult.error}`)
    }    
    console.log('total rows calculated', Number(totalRowsResult.queryResult.rows[0].count))
    return Number(totalRowsResult.queryResult.rows[0].count)
}

const importAndIngestEvent = async (
    payload: ImportEventsJobPayload,
    meta: PluginMeta<RedshiftImportPlugin>
) => {
    
    console.log('first step')
    /*
    const { global, cache, config, jobs } = meta
    console.log('Launched job #', payload.successiveRuns)
    
    const totalRowsToImport = await getTotalRowsToImport(config);
    
    console.log('Total rows :', totalRowsToImport)
    
    if (totalRowsToImport < 1)  {
        console.log(`Nothing to import, scheduling next job in ${WHEN_DONE_NEXT_JOB_SCHEDULE_SECONDS} seconds`)
        await jobs
            .importAndIngestEvent({ ...payload, retriesPerformedSoFar: 0})
            .runIn(WHEN_DONE_NEXT_JOB_SCHEDULE_SECONDS, 'seconds')
        return
    }

    const query = `SELECT * FROM ${sanitizeSqlIdentifier(
        config.tableName
    )}
    WHERE NOT EXISTS (
        SELECT 1 FROM ${sanitizeSqlIdentifier(config.eventLogTableName)} 
        WHERE ${sanitizeSqlIdentifier(config.tableName)}.event_id = ${sanitizeSqlIdentifier(config.eventLogTableName)}.event_id
        )
    LIMIT ${EVENTS_PER_BATCH}`

    const queryResponse = await executeQuery(query, [], config)
    if (!queryResponse || queryResponse.error || !queryResponse.queryResult) {
        const nextRetrySeconds = 2 ** payload.retriesPerformedSoFar * 3
        console.log(
            `Unable to process rows. Retrying in ${nextRetrySeconds}. Error: ${queryResponse.error}`
        )
        await jobs
            .importAndIngestEvent({ ...payload, retriesPerformedSoFar: payload.retriesPerformedSoFar + 1 })
            .runIn(nextRetrySeconds, 'seconds')
        return 
    }

    const eventsToIngest: TransformedPluginEvent[] = []
    const failedEvents : TransformedPluginEvent[] = []

    for (const row of queryResponse.queryResult!.rows) {
        const event = await transformations[TRANSFORMATION_NAME].transform(row, meta)
        
        if (event.isSuccessfulParsing) {
            eventsToIngest.push(event)
        } else {
            failedEvents.push(event)
        }
    }
    
    const eventIdsIngested = []    
    const eventIdsFailed = []  

    for (const event of eventsToIngest) {
        posthog.capture(event.event, event.properties)
        eventIdsIngested.push(event.id)
    }

    for (const event of failedEvents) {
        console.log('event failed:', event)
        eventIdsFailed.push(event.id)
        // regardless of parsing status, we add the event as ingested.
        // these events are also stoed in failedEvents, so we can always get back to them
        // we do this to not have to check 2 tables for events we already processed
        eventIdsIngested.push(event.id)
    }
    
    if (eventIdsIngested.length) {
        const joinedEventIds = eventIdsIngested.map(x => `('${x}', GETDATE())`).join(',')

        const insertQuery = `INSERT INTO ${sanitizeSqlIdentifier(
            meta.config.eventLogTableName
        )}
        (event_id, exported_at)
        VALUES
        ${joinedEventIds}`

        const insertQueryResponse = await executeQuery(insertQuery, [], config)        
    }

    if (eventIdsFailed.length) {
        const joinedFailedIds = eventIdsFailed.map(x => `('${x}', GETDATE())`).join(',')
        const insertFailedEventsQuery = `INSERT INTO ${sanitizeSqlIdentifier(
            meta.config.eventLogFailedTableName
        )}
        (event_id, attempted_at)
        VALUES
        ${joinedFailedIds}`

        const insertFailedEventsQueryResponse = await executeQuery(insertFailedEventsQuery, [], config)        
    }
    
    
 
    if ((eventsToIngest.length + failedEvents.length) < EVENTS_PER_BATCH) { 
        console.log(`Finished importing all events, scheduling next job in ${WHEN_DONE_NEXT_JOB_SCHEDULE_SECONDS} seconds`)
        await jobs
            .importAndIngestEvent({ ...payload, retriesPerformedSoFar: 0})
            .runIn(WHEN_DONE_NEXT_JOB_SCHEDULE_SECONDS, 'seconds')
        return
    }

    await jobs.importAndIngestEvent({ retriesPerformedSoFar: 0, successiveRuns : payload.successiveRuns+1 })
               .runIn(1, 'seconds')
    return */
}

const transformations: TransformationsMap = {
    'default': {
        author: 'yakkomajuri',
        transform: async (row, _) => {

            const { event_id, timestamp, distinct_id, event, properties, set} = row
            let eventToIngest = {
                "event": event,
                id:event_id,
            }

            try {
                const parsing_properties = JSON.parse(properties)
                const parsing_set = JSON.parse(set)

                eventToIngest['properties'] = {
                    distinct_id,
                    timestamp,
                    ...parsing_properties,
                    "$set": {
                        ...parsing_set
                    }
                }    
                eventToIngest['isSuccessfulParsing'] = true  

            } catch (err) {
                console.log('failed row :', row, err)
                eventToIngest['isSuccessfulParsing'] = false 
            }

            return eventToIngest
        }
    },
    
    'users_group': {
        author: 'mgrn',
        transform: async (row, _) => {

            const { event_id, timestamp, distinct_id, event, properties} = row
            let eventToIngest = {
                "event": event,
                id:event_id,
            }

            try {
                const parsing_properties = JSON.parse(properties)
                const parsing_set = JSON.parse(set)

                eventToIngest['properties'] = {
                    distinct_id,
                    timestamp,
                    ...parsing_properties,
                }    
                eventToIngest['isSuccessfulParsing'] = true  

            } catch (err) {
                console.log('failed row :', row, err)
                eventToIngest['isSuccessfulParsing'] = false 
            }

            return eventToIngest
        }
    }
}
