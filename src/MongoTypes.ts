import {Timestamp} from "bson";

export enum ChangeOpType {
    replace = 'replace',
    insert = 'insert',
    update = 'update',
    delete = 'delete',
}


export interface ChangeStreamEventObject {
    _id: any;
    operationType: ChangeOpType
    ns: {db: string, coll: string},
    clusterTime?: Timestamp,
    documentKey: object & {_id: any},
    fullDocument?: any,
}

export interface InsertEventObject extends ChangeStreamEventObject {
}

export interface UpdateEventObject extends ChangeStreamEventObject {
    updateDescription: {
        updatedFields: object,
        removedFields: string[],
    }
}

export interface ReplaceEventObject extends ChangeStreamEventObject {
}
export interface DeleteEventObject extends ChangeStreamEventObject {
}
