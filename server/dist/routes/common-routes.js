"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const admin_routes_1 = __importDefault(require("./admin/admin-routes"));
const segment_routes_1 = __importDefault(require("./segments/segment-routes"));
const validator_1 = require("../common/validator");
const router = express_1.default.Router();
router.use("/admin", admin_routes_1.default);
router.use("/segment", validator_1.checkAuth, validator_1.isValid, segment_routes_1.default);
exports.default = router;
//# sourceMappingURL=common-routes.js.map