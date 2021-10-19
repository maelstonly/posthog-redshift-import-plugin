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
        eventsToIgnore: string
        orderByColumn: string
        eventLogTableName: string
        pluginLogTableName: string
        transformationName: string
        importMechanism: 'Import continuously' | 'Only import historical data'
    }
}>

const IS_CURRENTLY_IMPORTING = 'stripped_import_plugin'
const RUN_LIMIT = 20
const sanitizeSqlIdentifier = (unquotedIdentifier: string): string => {
    return unquotedIdentifier
}

export const jobs: RedshiftImportPlugin['jobs'] = {
    randomJobJean: async (payload, meta) => await randomJobJean(payload as ImportEventsJobPayload, meta)
}


export const setupPlugin: RedshiftImportPlugin['setupPlugin'] = async ({ config, cache, jobs, global, storage }) => {
    console.log('setupPlugin')

    const initialValue = await storage.get(IS_CURRENTLY_IMPORTING)
    
    console.log('initialValue',initialValue)
    
    if (initialValue === true) {
        console.log('EXIT due to initial value = true')
        return
    }
    
    await storage.set(IS_CURRENTLY_IMPORTING, true)

    console.log('launching job')
    await jobs.randomJobJean({ successiveRuns: 0 }).runIn(10, 'seconds')
    console.log('done launching job')
   
}

export const teardownPlugin: RedshiftImportPlugin['teardownPlugin'] = async ({ global, cache, storage }) => {
    console.log('teardown')
    await storage.set(IS_CURRENTLY_IMPORTING, false)
    console.log('done tearing down')
}

const randomJobJean = async (
    payload: ImportEventsJobPayload,
    meta: PluginMeta<RedshiftImportPlugin>
) => {
    const { global, cache, config, jobs } = meta
    console.log('randomJobJean ', payload.successiveRuns)
    if (payload.successiveRuns >= RUN_LIMIT) {
        console.log(`done with ${RUN_LIMIT} calls, returning`)
        return
    }
    await jobs.randomJobJean({ successiveRuns: payload.successiveRuns+1 }).runIn(10, 'seconds')
    console.log('randomJobJean ', payload.successiveRuns, ' after next call')
    return 
}