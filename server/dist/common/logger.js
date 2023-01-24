"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.loggerOptions = exports.logger = void 0;
const winston_1 = require("winston");
const express_winston_1 = require("express-winston");
express_winston_1.requestWhitelist.push("body");
const logger = (0, winston_1.createLogger)({
    level: "info",
    exitOnError: false,
    format: winston_1.format.combine(winston_1.format.timestamp(), winston_1.format.errors({ stack: true }), winston_1.format.simple()),
    transports: [new winston_1.transports.Console()],
    handleExceptions: true,
    defaultMeta: true,
});
exports.logger = logger;
const loggerOptions = {
    level: "info",
    transports: [new winston_1.transports.Console()],
    format: winston_1.format.combine(winston_1.format.timestamp(), winston_1.format.errors({ stack: true }), winston_1.format.simple()),
    meta: true,
    expressFormat: true,
    colorize: false,
    ignoreRoute: (req, res) => false, // optional: allows to skip some log messages based on request and/or response
};
exports.loggerOptions = loggerOptions;
// @ts-ignore
logger.time = (label) => console.time(label);
// @ts-ignore
logger.timeEnd = (label) => console.timeEnd(label);
//# sourceMappingURL=logger.js.map