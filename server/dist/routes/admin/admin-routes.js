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
const AdminRouteHandler = __importStar(require("./admin-route-handler"));
const validator_1 = require("../../common/validator");
const express_validator_1 = require("express-validator");
const router = express_1.default.Router();
const registerValidates = [
    (0, express_validator_1.body)("name")
        .exists()
        .withMessage("input does not exist")
        .trim()
        .not()
        .isEmpty()
        .withMessage("input is empty")
        .matches("^[a-zA-Z]+( [a-zA-Z]+)*$")
        .withMessage("input is not [a-zA-Z] with spaces")
        .isLength({ max: 50 })
        .withMessage("input is too long"),
    (0, express_validator_1.body)("email")
        .exists()
        .withMessage("input does not exist")
        .trim()
        .not()
        .isEmpty()
        .withMessage("input is empty")
        .isEmail()
        .withMessage("input is not email")
        .normalizeEmail(),
    (0, express_validator_1.body)("password")
        .exists()
        .withMessage("input does not exist")
        .trim()
        .not()
        .isEmpty()
        .withMessage("input is empty"),
];
const loginValidates = [
    (0, express_validator_1.body)("email")
        .exists()
        .withMessage("input does not exist")
        .trim()
        .not()
        .isEmpty()
        .withMessage("input is empty")
        .isEmail()
        .withMessage("input is not email")
        .normalizeEmail(),
    (0, express_validator_1.body)("password")
        .exists()
        .withMessage("input does not exist")
        .trim()
        .not()
        .isEmpty()
        .withMessage("input is empty"),
];
router
    .route("/register")
    .post(registerValidates, validator_1.isValid, AdminRouteHandler.register);
router.route("/login").post(loginValidates, validator_1.isValid, AdminRouteHandler.login);
exports.default = router;
//# sourceMappingURL=admin-routes.js.map