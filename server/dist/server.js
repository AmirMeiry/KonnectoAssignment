"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
var _a;
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const logger_1 = require("./common/logger");
const mongo_wrapper_1 = require("./common/db/mongo-wrapper");
const middleware_base_1 = __importDefault(require("./middleware/middleware-base"));
const app = (0, express_1.default)();
const port = Number((_a = process.env.APP_PORT) !== null && _a !== void 0 ? _a : 8000);
// override console with winston.
console.log = (...args) => logger_1.logger.info.apply(logger_1.logger, args);
console.error = (...args) => logger_1.logger.error.apply(logger_1.logger, args);
console.info = (...args) => logger_1.logger.info.apply(logger_1.logger, args);
console.warn = (...args) => logger_1.logger.warn.apply(logger_1.logger, args);
(0, mongo_wrapper_1.connectMongoConnector)().then(() => {
    app.use("/api/v1", middleware_base_1.default.configuration);
    app
        .listen(port, () => console.log(`server is listening on ${port}`))
        .on("error", (err) => console.error((err === null || err === void 0 ? void 0 : err.message) || err));
});
//# sourceMappingURL=server.js.map