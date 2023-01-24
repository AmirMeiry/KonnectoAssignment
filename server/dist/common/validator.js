"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.isValid = exports.paramMongoId = exports.queryMongoId = exports.checkAuth = void 0;
const express_validator_1 = require("express-validator");
exports.checkAuth = [
    /* Konnecto Auth Header */
    (0, express_validator_1.header)("authorization")
        .exists()
        .withMessage("input does not exist")
        .trim()
        .not()
        .isEmpty()
        .withMessage("input is empty"),
];
exports.queryMongoId = [
    (0, express_validator_1.query)("id")
        .exists()
        .withMessage("input does not exist")
        .trim()
        .not()
        .isEmpty()
        .withMessage("input is empty")
        .isMongoId()
        .withMessage("Input not object id"),
];
exports.paramMongoId = [
    (0, express_validator_1.param)("id")
        .exists()
        .withMessage("input does not exist")
        .trim()
        .not()
        .isEmpty()
        .withMessage("input is empty")
        .isMongoId()
        .withMessage("Input not object id"),
];
function isValid(req, res, next) {
    const errors = (0, express_validator_1.validationResult)(req);
    if (!errors.isEmpty()) {
        console.error(`Validation Error in route: ${req.originalUrl}, errors are: \n${JSON.stringify(errors.mapped())}`);
        /* 422 Unprocessable Entity */
        const messages = errors.array().map((err) => err.msg);
        return res.status(422).json({ msg: messages[0] });
    }
    else {
        next();
    }
}
exports.isValid = isValid;
//# sourceMappingURL=validator.js.map