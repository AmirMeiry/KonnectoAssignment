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
exports.login = exports.register = void 0;
const route_error_handler_1 = require("../route-handlers/route-error-handler");
const mongo_wrapper_1 = require("../../common/db/mongo-wrapper");
function register(req, res) {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            const newUser = {
                created_time: new Date(),
                last_active: new Date(),
                email: req.body.email,
                name: req.body.name,
                password: req.body.password,
            };
            const adminCollection = yield (yield (0, mongo_wrapper_1.getDbWrapper)()).getCollection("admin");
            const userInsertObj = yield adminCollection.insertOne(newUser);
            res.json({ success: true, token: String(userInsertObj.insertedId) });
        }
        catch (error) {
            (0, route_error_handler_1.handleResponseError)(`User register Error: ${error.message}`, error.message, res);
        }
    });
}
exports.register = register;
function login(req, res) {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            // always login successfully for this assignment
            res.json({
                name: "Assignment user",
                token: "123",
            });
            return;
            const adminCollection = yield (yield (0, mongo_wrapper_1.getDbWrapper)()).getCollection("admin");
            const user = yield adminCollection.findOne({
                email: req.body.email,
                password: req.body.password,
            });
            if (user) {
                res.json({
                    name: user.name,
                    token: String(user._id),
                });
            }
            else {
                res.statusCode = 403;
                (0, route_error_handler_1.handleResponseError)(`User login Error: Wrong email or password`, "Wrong email or password", res);
            }
        }
        catch (error) {
            (0, route_error_handler_1.handleResponseError)(`User login Error: ${error.message}`, error.message, res);
        }
    });
}
exports.login = login;
//# sourceMappingURL=admin-route-handler.js.map