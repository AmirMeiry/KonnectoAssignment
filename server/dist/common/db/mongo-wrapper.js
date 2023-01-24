"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getDbWrapper = exports.connectMongoConnector = exports.TYPE = void 0;
const mongodb_1 = require("mongodb");
exports.TYPE = "mongo";
class MongoConnector {
    constructor(dbConfig) {
        this.type = exports.TYPE;
        this._defaultMaxTimeMS = 4000;
        if (!dbConfig) {
            throw new Error("Validation error: Missing field: 'dbConfig'");
        }
        if (!dbConfig.connectionURL) {
            throw new Error("Validation error: dbConfig is missing field: 'connectionURL'");
        }
        this._config = dbConfig;
        this._defaultMaxTimeMS = this._config.defaultMaxTimeMS || 4000;
        this._mongoConnector = mongodb_1.MongoClient.connect(this._config.connectionURL, {
            useNewUrlParser: true,
            useUnifiedTopology: true,
        });
    }
    getDbAndExecuteFunc(func, options) {
        return __awaiter(this, void 0, void 0, function* () {
            const localOptions = options || {};
            const maxAttempts = this._config.connectionMaxAttempts || 3;
            let lastError = null;
            for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
                try {
                    if (this._mongoConnector) {
                        const dbClient = yield this._mongoConnector;
                        return yield func(dbClient.db(), {
                            defaultMaxTimeMS: this._defaultMaxTimeMS,
                        });
                    }
                }
                catch (err) {
                    console.error(`error executing query ${err === null || err === void 0 ? void 0 : err.message}`);
                    if ((err.message &&
                        err.message.toLowerCase() === "topology was destroyed") ||
                        (err.name && err.name === "MongoNetworkError")) {
                        lastError = err;
                        this._mongoConnector = mongodb_1.MongoClient.connect(this._config.connectionURL, { useNewUrlParser: true });
                        continue;
                    }
                    if (err.message &&
                        err.message.toLowerCase() === "operation exceeded time limit") {
                        lastError = err;
                        continue;
                    }
                    throw err;
                }
            }
            console.warn(`MongoConnector. Exceed max attempts. Max attempts: ${maxAttempts}, operation: ${options.callerFuncName}`);
            throw lastError;
        });
    }
    getById(id, collectionName, props = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            const localProps = props || {};
            const queryOptions = {};
            if (!id) {
                throw new Error("id parameter is missing");
            }
            if (!collectionName) {
                throw new Error("collection name is missing");
            }
            if (props.projection) {
                queryOptions.projection = props.projection;
            }
            return this.getDbAndExecuteFunc((dbRef, { defaultMaxTimeMS }) => {
                return dbRef
                    .collection(collectionName)
                    .findOne({ _id: this.castToObjectId(id) }, Object.assign({ maxTimeMS: localProps.maxTimeMS || defaultMaxTimeMS }, queryOptions));
            }, `getById - ${collectionName}`);
        });
    }
    updateById(id, collectionName, updateBody, updateOptions, props = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!id) {
                throw new Error("id parameter is missing");
            }
            if (!collectionName) {
                throw new Error("collection name is missing");
            }
            return yield this.getDbAndExecuteFunc((dbRef, { defaultMaxTimeMS }) => {
                return dbRef
                    .collection(collectionName)
                    .updateOne({ _id: this.castToObjectId(id) }, updateBody, updateOptions);
            }, `getById - ${collectionName}`);
        });
    }
    updateOne(collectionName, query, updateBody, updateOptions, props = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!collectionName) {
                throw new Error("collection name is missing");
            }
            // todo result type.
            return this.getDbAndExecuteFunc((dbRef, { defaultMaxTimeMS }) => {
                return dbRef
                    .collection(collectionName)
                    .updateOne(query, updateBody, updateOptions);
            }, `updateOne - ${collectionName}`);
        });
    }
    getCollection(collectionName, props = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!collectionName) {
                throw new Error("collection name is missing");
            }
            return (yield this._mongoConnector).db().collection(collectionName);
        });
    }
    /**
     * @return mongodb ObjectId
     * */
    castToObjectId(value, options = {}) {
        return mongodb_1.ObjectId.isValid(value) ? new mongodb_1.ObjectId(value) : value;
    }
    /**
     * @return array of mongodb ObjectIds
     * */
    castArrayToObjectIds(ids, options = {}) {
        return ids.map((id) => {
            return mongodb_1.ObjectId.isValid(id) ? new mongodb_1.ObjectId(id) : id;
        });
    }
    close(options) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                const db = yield this._mongoConnector;
                if (db) {
                    yield db.close();
                }
            }
            catch (err) {
                console.error(err);
            }
            this._mongoConnector = null;
        });
    }
}
exports.default = MongoConnector;
let dbWrapper;
function connectMongoConnector() {
    return __awaiter(this, void 0, void 0, function* () {
        if (!dbWrapper) {
            dbWrapper = new MongoConnector({
                connectionURL: process.env.MONGO_CONNECTION_STRING,
            });
        }
    });
}
exports.connectMongoConnector = connectMongoConnector;
function getDbWrapper() {
    return dbWrapper;
}
exports.getDbWrapper = getDbWrapper;
//# sourceMappingURL=mongo-wrapper.js.map