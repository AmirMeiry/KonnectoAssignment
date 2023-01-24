"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.handleResponseError = void 0;
function handleResponseError(error, msg, res) {
    const result = { success: false, msg, error: "" };
    switch (res.statusCode) {
        case 418:
            result.error = "expired";
            console.warn(error);
            break;
        case 401:
        case 403:
            console.warn(error);
            result.error = "unauthorized";
            break;
        default:
            console.error(error);
            res.statusCode = 500;
            result.error = error;
    }
    res.json(result);
}
exports.handleResponseError = handleResponseError;
//# sourceMappingURL=route-error-handler.js.map