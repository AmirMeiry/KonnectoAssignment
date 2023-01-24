"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const helmet_1 = __importDefault(require("helmet"));
const rate_limits_1 = require("../common/rate-limits");
const logger_1 = require("../common/logger");
const expressWinston = __importStar(require("express-winston"));
const common_routes_1 = __importDefault(require("../routes/common-routes"));
class MiddlewareBase {
    static get configuration() {
        const app = (0, express_1.default)();
        app.enable("trust proxy");
        app.use(rate_limits_1.RateLimitsConfig);
        app.use((0, helmet_1.default)());
        app.use(expressWinston.logger(logger_1.loggerOptions));
        app.use(express_1.default.json({ limit: "5mb" }));
        app.use(express_1.default.urlencoded({ limit: "5mb", extended: true }));
        app.use(common_routes_1.default);
        return app;
    }
}
exports.default = MiddlewareBase;
Object.seal(MiddlewareBase);
//# sourceMappingURL=middleware-base.js.map