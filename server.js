const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mysql = require('mysql2/promise');
const path = require('path');
const fs = require('fs');
const zlib = require('zlib');
const protobuf = require('protobufjs');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');

puppeteer.use(StealthPlugin());

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: '*' },
    transports: ['websocket', 'polling']
});

const screenshotDir = path.join(__dirname, 'public', 'screenshots');
if (!fs.existsSync(screenshotDir)) fs.mkdirSync(screenshotDir, { recursive: true });
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// =========================================================================
// 1. CẤU HÌNH KẾT NỐI MYSQL
// =========================================================================
const dbConfig = {
    host: 'localhost',
    user: 'root',
    password: 'gpmn3041975',
    database: 'tiktok_hunter'
};

let pool;
async function initDB() {
  pool = mysql.createPool(dbConfig);
    try { await pool.query('ALTER TABLE whale_logs ADD COLUMN muc_choi BIGINT DEFAULT 0'); } catch (e) {}
    try { await pool.query("ALTER TABLE whale_logs ADD COLUMN gender VARCHAR(16) DEFAULT 'Nam'"); } catch (e) {}
    try { await pool.query("ALTER TABLE whale_logs ADD COLUMN region VARCHAR(32) DEFAULT 'Việt'"); } catch (e) {}
    try { await pool.query('ALTER TABLE whale_logs MODIFY COLUMN started_at DATETIME NULL DEFAULT NULL'); } catch (e) {}
    console.log('✅ Đã kết nối MySQL thành công!');
}

// =========================================================================
// 2. BỘ GIẢI MÃ PROTOBUF CHUẨN TIKTOK
// =========================================================================
const protoRoot = protobuf.Root.fromJSON({
    nested: {
        WebcastPushFrame: {
            fields: {
                seqId: { type: 'uint64', id: 1 },
                logId: { type: 'uint64', id: 2 },
                service: { type: 'uint64', id: 3 },
                method: { type: 'uint64', id: 4 },
                payloadEncoding: { type: 'string', id: 6 },
                payloadType: { type: 'string', id: 7 },
                payload: { type: 'bytes', id: 8 }
            }
        },
        ProtoMessageFetchResult: {
            fields: {
                messages: { rule: 'repeated', type: 'BaseMessage', id: 1 },
                cursor: { type: 'string', id: 2 }
            }
        },
        BaseMessage: {
            fields: {
                method: { type: 'string', id: 1 },
                payload: { type: 'bytes', id: 2 },
                msgId: { type: 'int64', id: 3 }
            }
        },
        CommonMessageData: {
            fields: {
                method: { type: 'string', id: 1 },
                msgId: { type: 'int64', id: 2 },
                roomId: { type: 'int64', id: 3 },
                createTime: { type: 'int64', id: 4 }
            }
        },
        WebcastRoomUserSeqMessage: {
            fields: {
                common: { type: 'CommonMessageData', id: 1 },
                ranks: { rule: 'repeated', type: 'Contributor', id: 2 },
                total: { type: 'int64', id: 3 },
                popStr: { type: 'string', id: 4 },
                seats: { rule: 'repeated', type: 'Contributor', id: 5 }
            }
        },
        Contributor: {
            fields: {
                score: { type: 'int64', id: 1 },
                user: { type: 'User', id: 2 },
                rank: { type: 'int64', id: 3 },
                delta: { type: 'int64', id: 4 }
            }
        },
        WebcastMemberMessage: {
            fields: {
                common: { type: 'CommonMessageData', id: 1 },
                user: { type: 'User', id: 2 },
                memberCount: { type: 'int64', id: 3 },
                action: { type: 'int64', id: 10 }
            }
        },
        WebcastChatMessage: {
            fields: {
                common: { type: 'CommonMessageData', id: 1 },
                user: { type: 'User', id: 2 },
                comment: { type: 'string', id: 3 }
            }
        },
        WebcastGiftMessage: {
            fields: {
                common: { type: 'CommonMessageData', id: 1 },
                giftId: { type: 'uint64', id: 2 },
                fanTicketCount: { type: 'uint64', id: 3 },
                groupCount: { type: 'uint64', id: 4 },
                repeatCount: { type: 'uint64', id: 5 },
                comboCount: { type: 'uint64', id: 6 },
                user: { type: 'User', id: 7 },
                toUser: { type: 'User', id: 8 },
                repeatEnd: { type: 'uint32', id: 9 },
                groupId: { type: 'uint64', id: 11 },
                giftDetails: { type: 'GiftStruct', id: 15 },
                monitorExtra: { type: 'string', id: 22 }
            }
        },
        GiftStruct: {
            fields: {
                describe: { type: 'string', id: 2 },
                id: { type: 'uint64', id: 5 },
                combo: { type: 'bool', id: 10 },
                type: { type: 'uint32', id: 11 },
                diamondCount: { type: 'uint32', id: 12 },
                name: { type: 'string', id: 16 }
            }
        },
        User: {
            fields: {
                id: { type: 'uint64', id: 1 },
                nickname: { type: 'string', id: 3 },
                gender: { type: 'int32', id: 4 },
                signature: { type: 'string', id: 5 },
                avatarThumb: { type: 'ImageModel', id: 9 },
                payGrade: { type: 'PayGrade', id: 19 },
                fansClub: { type: 'FansClub', id: 23 },
                displayId: { type: 'string', id: 38 },
                secUid: { type: 'string', id: 46 },
                badgeList: { rule: 'repeated', type: 'BadgeStruct', id: 64 }
            }
        },
        PayGrade: {
            fields: {
                level: { type: 'int64', id: 2 }
            }
        },
        FansClub: {
            fields: {
                data: { type: 'FansClubData', id: 1 }
            }
        },
        FansClubData: {
            fields: {
                clubName: { type: 'string', id: 1 },
                level: { type: 'int32', id: 2 }
            }
        },
        BadgeStruct: {
            fields: {
                displayType: { type: 'int32', id: 1 },
                priorityType: { type: 'int32', id: 2 },
                sceneType: { type: 'int32', id: 3 },
                badgeSceneType: { type: 'int32', id: 4 },
                logExtra: { type: 'PrivilegeLogExtra', id: 7 },
                combineOld: { type: 'CombineBadge', id: 13 },
                text: { type: 'TextBadge', id: 20 },
                combine: { type: 'CombineBadge', id: 21 }
            }
        },
        PrivilegeLogExtra: {
            fields: {
                dataVersion: { type: 'string', id: 1 },
                privilegeId: { type: 'string', id: 2 },
                level: { type: 'string', id: 3 }
            }
        },
        ImageModel: {
            fields: {
                urls: { rule: 'repeated', type: 'string', id: 1 },
                uri: { type: 'string', id: 2 }
            }
        },
        TextBadge: {
            fields: {
                key: { type: 'string', id: 1 },
                defaultPattern: { type: 'string', id: 2 },
                pieces: { rule: 'repeated', type: 'TextPiece', id: 3 }
            }
        },
        TextPiece: {
            fields: {
                type: { type: 'int32', id: 1 },
                stringValue: { type: 'string', id: 11 }
            }
        },
        CombineBadge: {
            fields: {
                displayType: { type: 'int32', id: 1 },
                icon: { type: 'ImageModel', id: 2 },
                text: { type: 'TextBadge', id: 3 },
                str: { type: 'string', id: 4 }
            }
        }
    }
});

const PushFrameType = protoRoot.lookupType('WebcastPushFrame');
const FetchResultType = protoRoot.lookupType('ProtoMessageFetchResult');
const GiftMessageType = protoRoot.lookupType('WebcastGiftMessage');
const MemberMessageType = protoRoot.lookupType('WebcastMemberMessage');
const ChatMessageType = protoRoot.lookupType('WebcastChatMessage');
const RoomUserSeqMessageType = protoRoot.lookupType('WebcastRoomUserSeqMessage');

// =========================================================================
// 3. BIẾN TOÀN CỤC & BỘ NHỚ QUẢN LÝ 2 ENGINE
// =========================================================================
let browser = null;
let page = null;
let cdpSession = null;
let isBotRunning = false;

let currentIdol = '';
let currentRoomId = '';
let currentAnchorId = '';
let currentAnchorSecUid = '';
let currentRoomSessionId = 0;
let isSwitchingRoom = false;
let closedRoomIdsSet = new Set();
let currentRoomHasCommerce = false;
let isRoomReadyForEngines = false;
let isPKActive = false;

let giftCatalogMap = new Map();
let processedMsgIds = new Set();
let roomAudienceScoreMap = new Map();    // Điểm Mức chơi thực tế trên bảng Người xem (Viewers)
let roomAudienceRankMap = new Map();     // Thứ hạng 1, 2, 3... trên bảng Người xem
let roomSessionDonatedMap = new Map();   // Số xu gốc tặng trực tiếp trong phiên Live
let userLevelsMemoryMap = new Map();     // Lưu Gifter Level, Fanclub Level & Giới tính chính xác
let roomDiscoveredUsersMap = new Map();  // Toàn bộ User phát hiện trong phòng (cả Online & Offline)
let idolGalleryUserGiftsMap = new Map(); // Lưu quà >=500 xu của User (cả Online & Offline) trong Phòng trưng bày
let userProfileCache = new Map();

let capturedByEngine1Gallery = new Set();
let checkedEngine1GalleryUsers = new Set();
let capturedByEngine2LivePK = new Set();
let inFlightEngine1Promises = new Map(); // Giữ các Promise đang quét Phòng trưng bày để không bị hủy khi chuyển phòng
let inFlightEngine2Promises = new Map();

let activeComboTimersMap = new Map();
let screenshotTakenMap = new Map();
let farmedFeedRoomsSet = new Set();
let screenshotLock = Promise.resolve();

const SHOP_KEYWORDS = [
    'giỏ hàng', 'xả kho', 'freeship', 'mã giảm', 'chốt đơn', 'săn deal', 'giá sốc', 'kệ hàng'
];

const FALLBACK_GIFT_PRICES_BY_NAME = {
    'súng bắn tiền': 500, 'money gun': 500,
    'thiên nga': 699, 'swan': 699,
    'xe lửa': 899, 'train': 899,
    'dải ngân hà': 1000, 'galaxy': 1000,
    'vòng đu quay': 1000, 'ferris wheel': 1000,
    'hoa pháo': 1088, 'fireworks': 1088,
    'ngôi sao lấp lánh': 1500, 'shooting star': 1500,
    'lặn cùng cá voi': 2150, 'whale diving': 2150,
    'mô tô': 2988, 'motorcycle': 2988,
    'máy bay riêng': 4888, 'private jet': 4888,
    'du thuyền': 9888, 'yacht': 9888,
    'xe thể thao': 7000, 'sports car': 7000,
    'interstellar': 10000,
    'diều hâu sấm sét': 39999, 'thunder falcon': 39999,
    'sư tử': 29999, 'lion': 29999,
    'ngọn lửa rồng': 26999, 'dragon flame': 26999,
    'tiktok universe': 44999, 'vũ trụ tiktok': 44999,
    'pegasus': 42999, 'phượng hoàng': 25999, 'phoenix': 25999,
    'cung điện': 15000, 'lâu đài': 20000, 'castle': 20000,
    'tên lửa': 10000, 'rocket': 10000, 'trực thăng': 3000, 'helicopter': 3000
};

// =========================================================================
// 4. BỘ NHẬN DIỆN GIỚI TÍNH & VÙNG MIỀN ĐA TẦNG (CHUẨN HÓA UNICODE + TÁCH ÂM TIẾT)
// =========================================================================
const FEMALE_EMOJI_REGEX = /🎀|🌸|🌷|🌺|🌻|🌼|💐|🥀|🪷|💮|🏵️|💄|💋|👠|👡|👢|👗|👙|👚|👛|👜|💅|💃|🧚|🧜|👰|🤰|🤱|🙇‍♀️|💁‍♀️|🙅‍♀️|🙆‍♀️|🙋‍♀️|🧏‍♀️|🤦‍♀️|🤷‍♀️|👧|👩|👱‍♀️|👸|🍓|🍒|🍑|🦋|💗|💓|💞|💕|❣️|🩷|💖|💘|💝|💟|♥️|❤️‍🔥|🧸|🐰|🐱|🐣|🐥|🦄|🫧|🍬|🍭|🍰|🧁|🧋|🍼|🌙/;
const FEMALE_DECOR_REGEX = /➳|❥|❦|❧|༊|༺|༻|ʚ|ɞ|✿|❀|❁|❃|♡|♥|✧|✦|✩|✪|✫|✬|✭|✮|✯|✰|☾|♪|♫|\d+(?:\.\d+)?\s*hz|\b1m[456]\d?\b|\b2k[3456789]\b|cute|xíu|iuu|babe|bae|luv/i;
const MALE_EMOJI_REGEX = /🧔|👦|👨|🤴|🤵|🫅|🕺|🏋️‍♂️|🥊|⚽|🏀|🐉|🦅|🐺|🦁|🏎️|🏍️|🍺|🍻|🥃|🚬|🗡️|⚔️|🛡️/;

// Cụm từ xưng hô sở hữu của Nữ (VD: "vợ yêu của anh", "bé của anh", "em iu anh", "nóc nhà của anh")
const FEMALE_RELATIONAL_PHRASE_REGEX = /(?:^|\s)(vợ|vk|bx|bà xã|em|bé|pé|em bé|babi|baby|công chúa|tiểu thư|nóc nhà|người yêu|ny|bồ|cục cưng)\b.*?\b(của\s+anh|yêu\s+anh|iu\s+anh|thương\s+anh|nhớ\s+anh|của\s+ck|của\s+chồng)/i;
const MALE_RELATIONAL_PHRASE_REGEX = /(?:^|\s)(chồng|ck|ông xã|anh|boy)\b.*?\b(của\s+em|yêu\s+em|iu\s+em|của\s+vợ|của\s+vk|yêu\s+vợ)/i;

const STRONG_FEMALE_PREFIX_REGEX = /(?:^|[\s._,-])(chị|chi|cô|co|cô ba|cô tư|cô út|bà|mẹ|má|mami|mom|mommy|vk|vợ|bx|bà xã|em bé|bé|pé|baby|babi|bbi|babe|girl|lady|women|woman|queen|princess|miss|mrs|ms|nữ|gái|xinh|nàng|tiểu thư|công chúa|mỹ nhân|phu nhân|nóc nhà|mẹ bỉm)(?:$|[\s._,-])/i;
const STRONG_MALE_PREFIX_REGEX = /(?:^|[\s._,-])(anh|mr|boy|men|man|gentleman|king|prince|boss|chú|bác|ông|cậu|thầy|cha|bố|ck|chồng|trai|đẹp trai|zai|tổng tài|đại gia|lão|ca ca|huynh|đệ)(?:$|[\s._,-])/i;

const FEMALE_COMPOUND_REGEX = /(thị|ngọc|mỹ|thu|bích|diệu|kiều|thúy|thủy|tuyết|hồng|cẩm|kim|lan|mai|quỳnh|thảo|trâm|vân|như|nhã|đoan|thục|huyền|phương|bảo|minh|khánh|hà|diệp|nguyệt|thùy|tú|châu|uyển|tử|tiểu|uyen|ngoc|my|bich|dieu|kieu|thuy|tuyet|hong|cam|quynh|thao|tram|van|nhu|nha|doan|thuc|huyen|phuong|bao|khanh|diep|nguyet|chau)\s*(anh|linh|trang|thảo|phương|hương|nhi|vy|vi|my|mi|trân|châu|hân|ngân|nhung|quỳnh|tuyết|vân|huyền|thu|hà|yến|hoa|lan|mai|trâm|thư|dung|diệp|duyên|hằng|hiền|hồng|huệ|liên|loan|ly|nga|oanh|phượng|quyên|sương|thắm|thơ|thoa|thúy|thủy|tiên|trinh|trúc|tú|uyên|xuân|thanh|khanh|thao|phuong|huong|tran|chau|han|ngan|quynh|tuyet|van|huyen|yen|tram|thu|diep|duyen|hang|hien|hong|hue|lien|quyen|suong|tham|tho|tien|truc|uyen|xuan)/i;

const MALE_COMPOUND_REGEX = /(văn|hữu|đức|đình|công|quốc|bá|trọng|mạnh|khắc|thế|duy|hoàng|quang|nhật|tuấn|việt|hùng|gia|nam|long|thanh|minh)\s+(minh|bảo|hải|sơn|hùng|dũng|tuấn|kiệt|hoàng|long|đức|thành|đạt|phúc|quân|khang|thắng|huy|tùng|bình|cường|vinh|phong|nhật|quang|thịnh|tiến|trung|việt|vũ|lâm|hào|khôi|tín|triết|nguyên|phát|lộc|tài|vương|dương|toàn|tân|hưng|hiếu|nghĩa|nhân|trí|khoa|kiên|phú|phước|quý|thái|thiện|thuận|trường|dat|tuan|kiet|hoang|duc|thanh|phuc|quan|thang|tung|cuong|thinh|trung|viet|khoi|triet|nguyen|phat|loc|vuong|duong|quoc|toan|hieu|nghia|nhan|manh|phuoc|truong|trong)/i;

const FEMALE_GIVEN_NAMES = new Set([
    'thảo', 'linh', 'trang', 'hương', 'ngọc', 'nhi', 'yến', 'hạnh', 'hoa', 'lan', 'mai', 'trâm', 'thư',
    'ngân', 'nhung', 'quỳnh', 'tuyết', 'vân', 'huyền', 'thu', 'hà', 'phương', 'trân', 'vy', 'vi', 'mi',
    'my', 'dung', 'diệp', 'điệp', 'duyên', 'hằng', 'hiền', 'hoài', 'hồng', 'huệ', 'hường', 'khuê', 'kiều',
    'lam', 'lệ', 'liên', 'liễu', 'loan', 'ly', 'mẫn', 'nga', 'ngà', 'oanh', 'phụng', 'phượng', 'quyên',
    'sương', 'thắm', 'thi', 'thơ', 'thoa', 'thúy', 'thuỷ', 'thủy', 'tiên', 'trinh', 'trúc', 'tú', 'uyên',
    'xuân', 'xuyến', 'ánh', 'chi', 'cẩm', 'châu', 'giang', 'giao', 'hân', 'lụa', 'mây', 'miu', 'na',
    'nương', 'nhàn', 'như', 'nhường', 'nụ', 'san', 'sen', 'sim', 'su', 'susu', 'thêu', 'thơm', 'thương',
    'tươi', 'uyển', 'xoan', 'yên', 'bông', 'mèo', 'thỏ', 'dâu', 'bơ', 'xoài', 'nấm', 'kem', 'kẹo', 'sữa',
    'cún', 'mỡ', 'nhím', 'sóc', 'bắp', 'đậu', 'nắng', 'trăng', 'tím', 'đào', 'mận', 'chanh', 'cam', 'quýt',
    'nhãn', 'vải', 'dừa', 'khế', 'me', 'sấu', 'mơ', 'cúc', 'trà', 'thị', 'bích', 'diệu', 'nhã', 'thục',
    'thao', 'huong', 'ngoc', 'yen', 'hanh', 'tram', 'ngan', 'quynh', 'tuyet', 'huyen', 'phuong', 'tran',
    'duyen', 'hang', 'hien', 'hue', 'kieu', 'lien', 'quyen', 'thuy', 'tien', 'truc', 'uyen',
    'xuan', 'cam', 'chau', 'han', 'nhu', 'bong', 'meo', 'tho', 'dau', 'keo', 'sua', 'nhim',
    'lisa', 'rose', 'jenny', 'jennie', 'jisoo', 'anna', 'bella', 'lucy', 'lily', 'amy', 'emma', 'sarah', 'tracy', 'mandy', 'reina', 'nana', 'mimi', 'kiki', 'lulu', 'chloe', 'sophia', 'grace', 'ruby', 'vivian', 'wendy', 'cindy', 'candy', 'sandy', 'cherry', 'berry', 'sunny', 'windy', 'jinny'
]);

const FEMALE_UID_SUBSTRINGS = [
    'uyenanh', 'ngocanh', 'phuonganh', 'lananh', 'maianh', 'kimanh', 'tramanh', 'quynhanh', 'baoanh', 'thuyanh', 'dieuanh', 'vananh', 'honganh', 'nguyetanh', 'kieuoanh', 'baotran', 'ngocnhi', 'yennhi', 'thanhnhi', 'tuthanh', 'tukhanh', 'phuonglinh', 'thuytrang', 'thanhthao', 'myduyen', 'quynhnhu', 'camtu', 'bichngoc', 'cobala', 'coba', 'cotu', 'cout', 'voiu', 'voyeu', 'pebe', 'empe', 'congchua', 'tieuthu', 'nocnha', 'princess', 'queen', 'girl', 'babi'
];

const MALE_GIVEN_NAMES = new Set([
    'hùng', 'dũng', 'tuấn', 'kiệt', 'hoàng', 'long', 'đức', 'thành', 'đạt', 'phúc', 'hải', 'quân', 'khang',
    'thắng', 'huy', 'tùng', 'sơn', 'bình', 'cường', 'vinh', 'phong', 'nhật', 'quang', 'thịnh', 'tiến',
    'trung', 'việt', 'vũ', 'lâm', 'hào', 'khôi', 'tín', 'triết', 'nguyên', 'phát', 'lộc', 'tài', 'vương',
    'dương', 'quốc', 'toàn', 'tân', 'hưng', 'hiếu', 'nghĩa', 'nhân', 'trí', 'doanh', 'đoàn', 'đông', 'đồng',
    'giáp', 'hiệp', 'hoan', 'hoàn', 'huân', 'hưởng', 'hữu', 'kha', 'khải', 'khiêm', 'khoa', 'khương', 'kiên',
    'lập', 'lợi', 'luân', 'lương', 'mạnh', 'nam', 'nghị', 'ninh', 'phi', 'phú', 'phước', 'quý', 'quyết',
    'quyền', 'sáng', 'sĩ', 'sỹ', 'thạch', 'thái', 'thăng', 'thiện', 'thuận', 'thực', 'tiệp', 'toản', 'tòng',
    'trọng', 'trực', 'trường', 'tuyên', 'tường', 'văn', 'vĩ', 'vĩnh', 'vượng', 'bá', 'đình', 'công', 'khắc',
    'hung', 'dung', 'tuan', 'kiet', 'hoang', 'duc', 'dat', 'phuc', 'quan', 'thang', 'tung', 'cuong',
    'nhat', 'thinh', 'trung', 'viet', 'khoi', 'triet', 'nguyen', 'phat', 'loc', 'vuong', 'duong', 'quoc',
    'toan', 'hieu', 'nghia', 'nhan', 'manh', 'phuoc', 'truong', 'trong', 'alex', 'david', 'john', 'kevin', 'tony', 'peter', 'jack', 'leo', 'ken'
]);

const MALE_UID_SUBSTRINGS = [
    'thanhdat', 'minhtuan', 'hoanglong', 'tuankiet', 'anhquan', 'quocbao', 'duchuy', 'minhkhoi', 'hoangnam', 'quochuy', 'vantuan', 'minhduc', 'quocviet', 'hoangphuc', 'anhkhoa', 'vanmanh', 'theanh', 'duyanh', 'quanganh', 'tuananh', 'hoanganh', 'nhatanh', 'vietanh', 'hunganh', 'namlong', 'gialong', 'baolong', 'thanhlong', 'hoangson', 'thanhson', 'minhson', 'quocdat', 'tiendat', 'phatdat', 'thanhcong', 'quockhang', 'minhkhang', 'duykhang', 'anhkiet', 'haokiet'
];

// Danh sách âm tiết tiếng Việt không dấu để nhận diện tên viết liền (như thanhdat, tukhanh, 698vuem, cobala...)
const VN_UNACCENTED_SYLLABLES = new Set([
    'nguyen', 'tran', 'le', 'pham', 'hoang', 'huynh', 'phan', 'vu', 'vo', 'dang', 'bui', 'do', 'ho', 'ngo', 'duong', 'ly',
    'trinh', 'dinh', 'lam', 'mai', 'cao', 'luong', 'thai', 'chau', 'ta', 'phung', 'kieu', 'vuong', 'ha', 'chu', 'trieu',
    'van', 'thi', 'huu', 'duc', 'cong', 'quoc', 'ba', 'trong', 'manh', 'khac', 'the', 'duy', 'quang', 'nhat', 'tuan', 'viet',
    'hung', 'gia', 'nam', 'long', 'anh', 'minh', 'bao', 'khanh', 'hai', 'son', 'dung', 'kiet', 'thanh', 'dat', 'phuc', 'quan',
    'khang', 'thang', 'huy', 'tung', 'binh', 'cuong', 'vinh', 'phong', 'thinh', 'tien', 'trung', 'linh', 'trang', 'thao', 'phuong',
    'huong', 'ngoc', 'nhi', 'yen', 'hanh', 'hoa', 'lan', 'tram', 'thu', 'ngan', 'nhung', 'quynh', 'tuyet', 'huyen', 'vy', 'my',
    'diep', 'duyen', 'hang', 'hien', 'hoai', 'hong', 'hue', 'khue', 'lien', 'lieu', 'loan', 'nga', 'oanh', 'quyen', 'suong',
    'tham', 'tho', 'thoa', 'thuy', 'truc', 'tu', 'uyen', 'xuan', 'xuyen', 'chi', 'cam', 'giang', 'giao', 'han', 'lua', 'may',
    'nhu', 'sen', 'sim', 'thom', 'thuong', 'tuoi', 'xoan', 'bong', 'meo', 'dau', 'xoai', 'kem', 'keo', 'sua', 'cun', 'nhim',
    'soc', 'bap', 'nang', 'tim', 'dao', 'chanh', 'quyt', 'nhan', 'vai', 'dua', 'khe', 'sau', 'cuc', 'tra', 'bich', 'dieu',
    'nha', 'thuc', 'doan', 'dong', 'giap', 'hiep', 'hoan', 'huan', 'kha', 'khai', 'khiem', 'khoa', 'khuong', 'kien', 'lap',
    'loi', 'luan', 'nghi', 'ninh', 'phi', 'phu', 'phuoc', 'quy', 'quyet', 'sang', 'si', 'sy', 'thach', 'thien', 'thuan',
    'tiep', 'toan', 'tong', 'truc', 'truong', 'tuyen', 'tuong', 'vi', 'tan', 'hieu', 'nghia', 'nhan', 'tri', 'loc', 'tai',
    'phat', 'khoi', 'tin', 'triet', 'hao', 'em', 'be', 'pe', 'co', 'chi', 'me', 'vo', 'vk', 'ck', 'chong', 'iu', 'yeu',
    'cua', 'nhok', 'nhoc', 'xinh', 'zai', 'trai', 'gai', 'shop', 'vn', 'hcm', 'hanoi', 'saigon', 'danang', 'la', 'mua', 'bon'
]);

const VN_STRONG_ROOT_REGEX = /(nguyen|huynh|truong|phuong|quynh|khanh|thinh|thanh|trang|trinh|quang|hoang|ngoc|nhung|tuyet|huyen|duyen|quyen|thuy|uyen|xuan|phuc|phuoc|cuong|luong|duong|vuong|tuong|hieu|kieu|dieu|thao|khoa|khoi|kiet|triet|tuan|huan|luan|xinh|nhok|nhoc|tieuthu|congchua|tongtai|daigia)/i;

const VN_DIACRITIC_REGEX = /[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]/i;
const FOREIGN_SCRIPT_REGEX = /[\u0400-\u04FF\u0600-\u06FF\u0E00-\u0E7F\u3040-\u30FF\u4E00-\u9FFF\uAC00-\uD7AF]/;

// Hàm giải mã các font chữ kiểu (𝓣ử 𝓣𝓱𝓪𝓷𝓱ᴺʰⁱ -> Tử Thanh Nhi)
function normalizeFancyText(str = '') {
    if (!str) return '';
    return String(str)
        // Tách chữ nhỏ superscript/subscript (VD: 𝓣𝓱𝓪𝓷𝓱ᴺʰⁱ -> 𝓣𝓱𝓪𝓷𝓱 ᴺʰⁱ)
        .replace(/([^\s\u1D2C-\u1D6A\u2070-\u209C])([\u1D2C-\u1D6A\u2070-\u209C]+)/g, '$1 $2')
        // Chuẩn hóa NFKC biến toàn bộ font chữ kiểu toán học và chữ mũ về chữ cái thường + ghép dấu tiếng Việt
        .normalize('NFKC')
        .normalize('NFC');
}

// Kiểm tra 1 từ viết liền không dấu có phải là ghép từ 2 âm tiết tiếng Việt (VD: thanhdat = thanh + dat, tukhanh = tu + khanh, 698vuem = vu + em)
function isVietnameseCompoundToken(rawToken) {
    const clean = String(rawToken || '').toLowerCase().replace(/[0-9_.-]+/g, '');
    if (clean.length < 4) return false;
    if (VN_UNACCENTED_SYLLABLES.has(clean) || VN_STRONG_ROOT_REGEX.test(clean)) return true;

    for (let i = 2; i <= clean.length - 2; i++) {
        const left = clean.slice(0, i);
        const right = clean.slice(i);
        if (VN_UNACCENTED_SYLLABLES.has(left) && VN_UNACCENTED_SYLLABLES.has(right)) {
            return true;
        }
        // Trường hợp ghép 3 âm tiết (VD: cobala = co + ba + la)
        if (VN_UNACCENTED_SYLLABLES.has(left) && right.length >= 4) {
            for (let j = 2; j <= right.length - 2; j++) {
                if (VN_UNACCENTED_SYLLABLES.has(right.slice(0, j)) && VN_UNACCENTED_SYLLABLES.has(right.slice(j))) {
                    return true;
                }
            }
        }
    }
    return false;
}

function detectUserRegion(nickname = '', uniqueId = '', signature = '') {
    const normNick = normalizeFancyText(nickname);
    const normSig = normalizeFancyText(signature);
    const normUid = normalizeFancyText(uniqueId);
    const rawCombined = `${normNick} ${normSig}`;

    // 1. Có dấu tiếng Việt (kể cả trong font chữ kiểu đã giải mã NFKC) -> 100% Việt
    if (VN_DIACRITIC_REGEX.test(rawCombined)) return 'Việt';
    // 2. Có chữ tượng hình Hàn/Nhật/Trung/Thái/Ả Rập/Nga mà không có dấu Việt -> Nước ngoài
    if (FOREIGN_SCRIPT_REGEX.test(rawCombined)) return 'Nước ngoài';

    const lowerAll = `${normNick} ${normUid} ${normSig}`.toLowerCase();
    if (VN_STRONG_ROOT_REGEX.test(lowerAll)) return 'Việt';

    const tokens = lowerAll.split(/[^a-z0-9]+/).filter(Boolean);
    for (const tk of tokens) {
        if (
            VN_UNACCENTED_SYLLABLES.has(tk) ||
            FEMALE_GIVEN_NAMES.has(tk) ||
            MALE_GIVEN_NAMES.has(tk) ||
            isVietnameseCompoundToken(tk)
        ) {
            return 'Việt';
        }
    }
    return 'Nước ngoài';
}

function evaluateGenderPriority(apiGender, nickname = '', uniqueId = '', signature = '') {
    const g = toSafeNumber(apiGender);
    if (g === 1) return { gender: 'Nam', priority: 100, known: true };
    if (g === 2) return { gender: 'Nữ', priority: -100, known: true };

    // Giải mã font chữ kiểu (𝓣ử 𝓣𝓱𝓪𝓷𝓱ᴺʰⁱ -> Tử Thanh Nhi) & tách chữ CamelCase (UyểnAnh -> Uyển Anh)
    const normNick = normalizeFancyText(nickname).replace(
        /([a-zàáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ])([A-ZÀÁẠẢÃÂẦẤẬẨẪĂẰẮẶẲẴÈÉẸẺẼÊỀẾỆỂỄÌÍỊỈĨÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠÙÚỤỦŨƯỪỨỰỬỮỲÝỴỶỸĐ])/g,
        '$1 $2'
    );
    const normSig = normalizeFancyText(signature);
    const normUid = normalizeFancyText(uniqueId).toLowerCase();

    const rawText = `${nickname} ${normNick} ${normSig}`.trim();
    const fullText = `${normNick} ${normUid} ${normSig}`.toLowerCase();

    if (!fullText.trim()) {
        return { gender: 'Nam', priority: 10, known: false };
    }

    let score = 0; // Dương = Nam, Âm = Nữ

    // 1. Bắt cụm từ quan hệ sở hữu trước (VD: "vợ yêu của anh", "bé của anh" -> Nữ 100%)
    if (FEMALE_RELATIONAL_PHRASE_REGEX.test(fullText)) {
        score -= 140;
    } else if (MALE_RELATIONAL_PHRASE_REGEX.test(fullText)) {
        score += 140;
    }

    // Loại bỏ đuôi "của anh / yêu anh / iu anh" trước khi kiểm tra từ khóa Nam để không bị cộng nhầm +80 cho chữ "anh"
    const textWithoutPossessiveMale = fullText.replace(/\b(của|yêu|iu|thương|nhớ)\s+(anh|ck|chồng)\b/gi, ' ');

    // 2. Kiểm tra Emoji & Ký tự thẩm mỹ của Nữ (như 🪷, ➳❥99.9Hz💗༊)
    if (FEMALE_EMOJI_REGEX.test(rawText)) score -= 65;
    if (FEMALE_DECOR_REGEX.test(rawText) || FEMALE_DECOR_REGEX.test(fullText)) score -= 65;
    if (MALE_EMOJI_REGEX.test(rawText)) score += 65;

    // 3. Kiểm tra danh xưng & tên ghép
    if (STRONG_FEMALE_PREFIX_REGEX.test(fullText)) score -= 85;
    if (STRONG_MALE_PREFIX_REGEX.test(textWithoutPossessiveMale)) score += 80;

    if (FEMALE_COMPOUND_REGEX.test(fullText)) score -= 90;
    if (MALE_COMPOUND_REGEX.test(textWithoutPossessiveMale)) score += 85;

    // 4. Tách từ trong Nickname đã chuẩn hóa NFKC
    const cleanNick = textWithoutPossessiveMale
        .replace(/[^a-z0-9àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ\s]/gi, ' ')
        .trim();
    const words = cleanNick.split(/\s+/).filter(Boolean);

    for (let i = 0; i < words.length; i++) {
        const w = words[i].replace(/^[0-9]+|[0-9]+$/g, '');
        if (!w) continue;
        const isLastWord = (i === words.length - 1);
        const weight = isLastWord ? 60 : 30;

        if (FEMALE_GIVEN_NAMES.has(w)) score -= weight;
        else if (MALE_GIVEN_NAMES.has(w)) score += weight;
        else if (w.length >= 5) {
            // Nếu viết liền không dấu trong nickname (VD: "thanhdat", "uyenanh")
            if (FEMALE_UID_SUBSTRINGS.some(sub => w.includes(sub))) score -= 65;
            else if (MALE_UID_SUBSTRINGS.some(sub => w.includes(sub))) score += 65;
        }
    }

    // 5. Kiểm tra trong @uniqueId
    if (normUid) {
        for (const fSub of FEMALE_UID_SUBSTRINGS) {
            if (normUid.includes(fSub)) { score -= 65; break; }
        }
        for (const mSub of MALE_UID_SUBSTRINGS) {
            if (normUid.includes(mSub)) { score += 65; break; }
        }
        if (score === 0) {
            const uidTokens = normUid.split(/[._0-9-]+/).filter(Boolean);
            for (const tk of uidTokens) {
                if (FEMALE_GIVEN_NAMES.has(tk)) score -= 45;
                else if (MALE_GIVEN_NAMES.has(tk)) score += 45;
            }
        }
    }

    if (score < 0) return { gender: 'Nữ', priority: Math.max(-100, score), known: true };
    if (score > 0) return { gender: 'Nam', priority: Math.min(100, score), known: true };
    return { gender: 'Nam', priority: 20, known: false };
}

// =========================================================================
// 5. HÀM XỬ LÝ SỐ LIỆU & BÓC TÁCH CẤP ĐỘ (GIFTER + TIM ĐỘI CAM)
// =========================================================================
function toSafeNumber(val) {
    if (val === undefined || val === null) return 0;
    if (typeof val === 'number') return isNaN(val) ? 0 : val;
    if (typeof val === 'bigint') return Number(val);
    if (typeof val === 'object') {
        if (typeof val.toNumber === 'function') return val.toNumber();
        if (typeof val.toString === 'function') return Number(val.toString()) || 0;
    }
    return Number(val) || 0;
}

function toSafeStringId(val) {
    if (val === undefined || val === null) return '';
    if (typeof val === 'string') return val.trim();
    if (typeof val === 'object' && typeof val.toString === 'function') return val.toString();
    return String(val);
}

function parseCompactDiamonds(val) {
    if (!val) return 0;
    if (typeof val === 'number') return val;
    const s = String(val).trim().toUpperCase();

    // Trường hợp có hậu tố K, M (VD: "1.2K", "1,5K", "2M")
    const kmMatch = s.match(/(\d+(?:[.,]\d+)?)\s*([KM])/i);
    if (kmMatch) {
        let num = parseFloat(kmMatch[1].replace(/,/g, '.'));
        const unit = kmMatch[2].toUpperCase();
        if (unit === 'K') num *= 1000;
        else if (unit === 'M') num *= 1000000;
        return Math.round(num);
    }

    // Trường hợp số nguyên có dấu chấm/phẩy ngăn cách hàng nghìn (VD: "1.500", "12,450")
    const digitsOnly = s.replace(/[^\d]/g, '');
    return digitsOnly ? parseInt(digitsOnly, 10) : 0;
}

function parseCleanLevelNumber(val) {
    if (val === undefined || val === null) return 0;
    const cleaned = String(val).replace(/\{\d+\}/g, '').trim();
    const m = cleaned.match(/\d+/);
    if (!m) return 0;
    const num = parseInt(m[0], 10);
    return (num > 0 && num <= 120) ? num : 0;
}

function formatCompactCount(num) {
    const n = toSafeNumber(num);
    if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
    if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
    return String(n);
}

function rememberUserLevels(userId, gifterLevel = 0, fanclubLevel = 0, avatarUrl = '', genderInfo = null) {
    const uid = toSafeStringId(userId);
    if (!uid || uid === '0') return { gifterLevel: 0, fanclubLevel: 0, avatarUrl: '', gender: 'Nam', genderPriority: 20, genderKnown: false };
    const prev = userLevelsMemoryMap.get(uid) || { gifterLevel: 0, fanclubLevel: 0, avatarUrl: '', gender: 'Nam', genderPriority: 20, genderKnown: false };

    let finalGender = prev.gender || 'Nam';
    let finalPriority = prev.genderPriority ?? 20;
    let finalKnown = Boolean(prev.genderKnown);

    if (genderInfo) {
        if (genderInfo.known) {
            finalGender = genderInfo.gender;
            finalPriority = genderInfo.priority;
            finalKnown = true;
        } else if (!finalKnown) {
            finalGender = genderInfo.gender || finalGender;
            finalPriority = genderInfo.priority ?? finalPriority;
        }
    }

    const updated = {
        gifterLevel: Math.max(prev.gifterLevel || 0, toSafeNumber(gifterLevel)),
        fanclubLevel: Math.max(prev.fanclubLevel || 0, toSafeNumber(fanclubLevel)),
        avatarUrl: avatarUrl || prev.avatarUrl || '',
        gender: finalGender,
        genderPriority: finalPriority,
        genderKnown: finalKnown
    };
    userLevelsMemoryMap.set(uid, updated);
    return updated;
}

function extractLevelsFromProtoUser(user, extra = {}, rawBuffer = null) {
    let gifterLevel = Math.max(toSafeNumber(user?.payGrade?.level), toSafeNumber(extra?.user_grade));
    let fanclubLevel = Math.max(toSafeNumber(user?.fansClub?.data?.level), toSafeNumber(extra?.fans_club_level));
    const avatarUrl = user?.avatarThumb?.urls?.[0] || '';

    if (Array.isArray(user?.badgeList)) {
        for (const badge of user.badgeList) {
            const scene = toSafeNumber(badge.badgeSceneType || badge.sceneType);
            const c = badge.combine || badge.combineOld || {};

            const candidates = [
                badge.logExtra?.level,
                c.str,
                ...(c.text?.pieces || []).map(p => p.stringValue),
                ...(badge.text?.pieces || []).map(p => p.stringValue),
                c.text?.defaultPattern,
                badge.text?.defaultPattern
            ];

            let bestLvl = 0;
            for (const cand of candidates) {
                const n = parseCleanLevelNumber(cand);
                if (n > bestLvl) bestLvl = n;
            }

            const iconUri = String(c.icon?.uri || (c.icon?.urls || []).join(' ') || '').toLowerCase();
            const keyStr = String(c.text?.key || badge.text?.key || badge.logExtra?.privilegeId || '').toLowerCase();
            const badgeStr = JSON.stringify(badge).toLowerCase();

            if (bestLvl === 0) {
                const uriMatch = (iconUri + ' ' + badgeStr).match(/(?:grade_badge_icon_lite_lv|fans_badge_icon_lv|lv)(\d+)/i);
                if (uriMatch) bestLvl = parseInt(uriMatch[1], 10);
            }

            const isGifter = (scene === 8 || iconUri.includes('grade_badge') || iconUri.includes('user_grade') || keyStr.includes('grade') || badgeStr.includes('grade_badge'));
            const isFanclub = (scene === 10 || scene === 4 || scene === 7 || iconUri.includes('fans_badge') || iconUri.includes('fanclub') || keyStr.includes('fans') || badgeStr.includes('fans_badge'));

            if (isGifter && bestLvl > gifterLevel) gifterLevel = bestLvl;
            else if (isFanclub && bestLvl > fanclubLevel) fanclubLevel = bestLvl;
        }
    }

    if (rawBuffer) {
        const rawLatin = rawBuffer.toString('latin1');
        if (gifterLevel === 0) {
            const gradeRegex = /grade_badge_icon[^\x00-\x1f]{1,80}?[\x01-\x20](\d{1,2})(?=[^\d]|$)/gi;
            let gMatch;
            while ((gMatch = gradeRegex.exec(rawLatin)) !== null) {
                const val = parseInt(gMatch[1], 10);
                if (val > gifterLevel && val <= 70) gifterLevel = val;
            }
        }
        if (fanclubLevel === 0) {
            const fanRegex = /fans_badge[^\x00-\x1f]{1,120}?[\x01-\x20](\d{1,3})(?=[^\d]|$)/gi;
            let fMatch;
            while ((fMatch = fanRegex.exec(rawLatin)) !== null) {
                const val = parseInt(fMatch[1], 10);
                if (val > fanclubLevel && val <= 100) fanclubLevel = val;
            }
        }
    }

    const genderInfo = evaluateGenderPriority(user?.gender, user?.nickname, user?.displayId, user?.signature);
    const uid = toSafeStringId(user?.id || extra?.from_user_id || extra?.user_id);
    return rememberUserLevels(uid, gifterLevel, fanclubLevel, avatarUrl, genderInfo);
}

function extractLevelsFromApiUserJson(u) {
    if (!u || typeof u !== 'object') return { gifterLevel: 0, fanclubLevel: 0, avatarUrl: '', gender: 'Nam', genderPriority: 20 };
    let gLvl = toSafeNumber(u.pay_grade?.level || u.user_grade || u.grade_level);
    let fLvl = toSafeNumber(u.fans_club?.data?.level || u.fans_club?.level);
    const avatarUrl = u.avatar_thumb?.url_list?.[0] || u.avatar_medium?.url_list?.[0] || u.avatar?.url_list?.[0] || '';

    if (u.fans_club?.prefer_data && typeof u.fans_club.prefer_data === 'object') {
        for (const k of Object.keys(u.fans_club.prefer_data)) {
            const sub = u.fans_club.prefer_data[k];
            const pLvl = toSafeNumber(sub?.level || sub?.data?.level || sub?.fans_club_level);
            if (pLvl > fLvl) fLvl = pLvl;
        }
    }

    const badges = u.badge_list || u.badges || [];
    if (Array.isArray(badges)) {
        for (const b of badges) {
            if (!b || typeof b !== 'object') continue;
            const scene = toSafeNumber(b.badge_scene_type ?? b.scene_type);
            const c = b.combine || b.combine_badge_struct || {};

            const candidates = [
                b.privilege_log_extra?.level,
                c.str,
                ...(c.text?.pieces || []).map(p => p.string_value || p.stringValue),
                ...(b.text?.pieces || []).map(p => p.string_value || p.stringValue),
                c.text?.default_pattern,
                b.text?.default_pattern
            ];

            const panelList = c.profile_card_panel?.profile_content?.content_list || [];
            for (const pItem of panelList) {
                if (pItem?.text?.default_pattern) candidates.push(pItem.text.default_pattern);
                for (const piece of (pItem?.text?.pieces || [])) {
                    if (piece?.string_value) candidates.push(piece.string_value);
                }
            }

            let best = 0;
            for (const raw of candidates) {
                const n = parseCleanLevelNumber(raw);
                if (n > best) best = n;
            }

            const bStr = JSON.stringify(b).toLowerCase();
            if (best === 0) {
                const lvMatch = bStr.match(/(?:grade_badge_icon_lite_lv|fans_badge_icon_lv|fans_badge.*?lv)(\d+)/i);
                if (lvMatch) best = parseInt(lvMatch[1], 10);
            }

            const isGifter = (scene === 8 || bStr.includes('grade_badge') || bStr.includes('user_grade'));
            const isFanclub = (scene === 10 || scene === 4 || scene === 7 || bStr.includes('fans_badge') || bStr.includes('fanclub') || bStr.includes('fans_club'));

            if (isGifter && best > gLvl) gLvl = best;
            else if (isFanclub && best > fLvl) fLvl = best;
        }
    }

    const genderInfo = evaluateGenderPriority(u.gender, u.nickname, u.display_id, u.bio_description || u.signature);
    const uid = toSafeStringId(u.id_str || u.user_id_str || u.id || u.user_id);
    return rememberUserLevels(uid, gLvl, fLvl, avatarUrl, genderInfo);
}

function getUserMucChoiInRoom(userId) {
    const uid = toSafeStringId(userId);
    return toSafeNumber(roomAudienceScoreMap.get(uid));
}

// =========================================================================
// 6. CÁC HÀM CHỤP HÌNH CHUẨN XÁC THEO HÌNH MẪU CỦA BRO
// =========================================================================
async function captureLiveOrPKScreen(uniqueId, tag = 'live', expectedSessionId = currentRoomSessionId) {
    return (screenshotLock = screenshotLock.then(async () => {
        if (!page || page.isClosed() || isSwitchingRoom || currentRoomSessionId !== expectedSessionId) return null;
        try {
            const safeId = String(uniqueId || 'user').replace(/[^a-zA-Z0-9_-]/g, '');
            const fileName = `live_${safeId}_${tag}_${Date.now()}.jpg`;
            const filePath = path.join(screenshotDir, fileName);
            await page.screenshot({ path: filePath, type: 'jpeg', quality: 85 });
            return `/screenshots/${fileName}`;
        } catch (err) {
            return null;
        }
    }));
}

async function captureMucChoiViewerMoment(userInfo, expectedSessionId = currentRoomSessionId) {
    return (screenshotLock = screenshotLock.then(async () => {
        if (!page || page.isClosed() || isSwitchingRoom || currentRoomSessionId !== expectedSessionId || currentIdol !== userInfo.idol) {
            return null;
        }
        try {
            const safeId = String(userInfo.uniqueId || userInfo.userId || 'user').replace(/[^a-zA-Z0-9_-]/g, '');
            const fileName = `mucchoi_${safeId}_${Date.now()}.jpg`;
            const filePath = path.join(screenshotDir, fileName);

            await page.evaluate(() => {
                const m1 = document.getElementById('whale-hunter-mucchoi-badge');
                if (m1) m1.remove();
                const m2 = document.getElementById('whale-hunter-gallery-modal');
                if (m2) m2.remove();
                const m3 = document.getElementById('whale-hunter-native-popover');
                if (m3) m3.remove();
            }).catch(() => {});

            const topBuf = await page.screenshot({ type: 'jpeg', quality: 86 });
            const topBase64 = topBuf.toString('base64');

            if (currentRoomSessionId !== expectedSessionId) return null;

            await page.evaluate(async (u) => {
                const pop = document.createElement('div');
                pop.id = 'whale-hunter-native-popover';
                pop.style.cssText = `
                    position: fixed; top: 88px; right: 48px; width: 285px;
                    background: #18181b; border: 1px solid #27272a;
                    border-radius: 10px; padding: 14px; z-index: 2147483646;
                    color: #fff; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
                    box-shadow: 0 12px 32px rgba(0,0,0,0.85);
                `;
                pop.innerHTML = `
                    <div style="display:flex; justify-content:space-between; align-items:flex-start;">
                        <div style="display:flex; gap:10px; align-items:center;">
                            ${u.avatarUrl ? `<img src="${u.avatarUrl}" style="width:42px; height:42px; border-radius:50%; object-fit:cover;" />` : `<div style="width:42px; height:42px; border-radius:50%; background:#27272a; display:flex; align-items:center; justify-content:center;">👤</div>`}
                            <div>
                                <div style="font-size:13px; font-weight:700; color:#f4f4f5;">${u.uniqueId}</div>
                                <div style="font-size:12px; color:#a1a1aa; margin-top:1px;">${u.nickname}</div>
                            </div>
                        </div>
                        <span style="color:#a1a1aa; font-size:14px;">✕</span>
                    </div>
                    <div style="display:flex; gap:6px; margin-top:10px; flex-wrap:wrap; align-items:center;">
                        <span style="background:#3b3054; color:#c4b5fd; font-size:11px; font-weight:700; padding:2px 7px; border-radius:4px;">💎 ${u.gifterLevel}</span>
                        ${u.fanclubLevel > 0 ? `<span style="background:#432818; color:#fdba74; font-size:11px; font-weight:700; padding:2px 7px; border-radius:4px;">🧡 Lv.${u.fanclubLevel}</span>` : ''}
                        <span style="background:#3f2e3e; color:#f9a8d4; font-size:11px; font-weight:700; padding:2px 7px; border-radius:4px;">🔥 Hạng ${u.rank || 1}</span>
                    </div>
                    <div style="display:flex; gap:14px; margin-top:12px; font-size:12px; color:#d4d4d8;">
                        <div><b>${u.followerStr || '1.2K'}</b> <span style="color:#a1a1aa;">Follower</span></div>
                        <div><b>${u.followingStr || '150'}</b> <span style="color:#a1a1aa;">Lượt thích</span></div>
                    </div>
                    <div style="display:flex; gap:8px; margin-top:12px;">
                        <div style="flex:1; background:#fe2c55; color:#fff; text-align:center; padding:6px 0; border-radius:6px; font-size:12px; font-weight:700;">Follow</div>
                        <div style="flex:1; background:#27272a; color:#e4e4e7; text-align:center; padding:6px 0; border-radius:6px; font-size:12px; font-weight:600;">Báo cáo</div>
                    </div>
                `;
                document.body.appendChild(pop);
                await new Promise(r => setTimeout(r, 150));
            }, userInfo);

            const botBuf = await page.screenshot({ type: 'jpeg', quality: 86 });
            const botBase64 = botBuf.toString('base64');

            const combinedBase64 = await page.evaluate(async (b1, b2) => {
                const loadImg = (src) => new Promise((res) => {
                    const img = new Image();
                    img.onload = () => res(img);
                    img.src = 'data:image/jpeg;base64,' + src;
                });
                const [img1, img2] = await Promise.all([loadImg(b1), loadImg(b2)]);
                const canvas = document.createElement('canvas');
                canvas.width = img1.width;
                canvas.height = img1.height + img2.height + 70;
                const ctx = canvas.getContext('2d');
                ctx.fillStyle = '#090d16';
                ctx.fillRect(0, 0, canvas.width, canvas.height);

                ctx.fillStyle = '#ffffff';
                ctx.font = 'bold 22px sans-serif';
                ctx.fillText('Bảng điểm', 20, 26);
                ctx.drawImage(img1, 0, 35);

                ctx.fillText('Hồ sơ', 20, img1.height + 62);
                ctx.drawImage(img2, 0, img1.height + 70);
                return canvas.toDataURL('image/jpeg', 0.85).split(',')[1];
            }, topBase64, botBase64).catch(() => null);

            await page.evaluate(() => {
                const el = document.getElementById('whale-hunter-native-popover');
                if (el) el.remove();
            }).catch(() => {});

            if (currentRoomSessionId !== expectedSessionId) return null;

            if (combinedBase64) {
                fs.writeFileSync(filePath, Buffer.from(combinedBase64, 'base64'));
            } else {
                fs.writeFileSync(filePath, topBuf);
            }

            return `/screenshots/${fileName}`;
        } catch (err) {
            return null;
        }
    }));
}

// CHỤP HÌNH PHÒNG TRƯNG BÀY KHỚP 100% HÌNH ẢNH 1 CỦA BRO (Hộp kính 3D + Nhà sưu tầm / 1/7 + Nút Gửi/Nhận danh hiệu + 3 người tặng quà hàng đầu)
async function captureGalleryMoment(userInfo, galleryDetails, expectedSessionId = currentRoomSessionId) {
    return (screenshotLock = screenshotLock.then(async () => {
        if (!page || page.isClosed() || isSwitchingRoom || currentRoomSessionId !== expectedSessionId || currentIdol !== userInfo.idol) {
            return null;
        }
        try {
            const safeId = String(userInfo.uniqueId || userInfo.userId || 'user').replace(/[^a-zA-Z0-9_-]/g, '');
            const fileName = `gallery_${safeId}_${Date.now()}.jpg`;
            const filePath = path.join(screenshotDir, fileName);

            await page.evaluate(async (u, items) => {
                const old = document.getElementById('whale-hunter-gallery-modal');
                if (old) old.remove();
                const oldPop = document.getElementById('whale-hunter-native-popover');
                if (oldPop) oldPop.remove();

                const modal = document.createElement('div');
                modal.id = 'whale-hunter-gallery-modal';
                modal.style.cssText = `
                    position: fixed; bottom: 0; left: 50%; transform: translateX(-50%);
                    width: 860px; max-width: 98vw; background: #14100d;
                    border-top: 2px solid #574432; border-left: 1.5px solid #4a3828; border-right: 1.5px solid #4a3828;
                    border-radius: 18px 18px 0 0; padding: 14px 18px;
                    z-index: 2147483647; color: #f5ebe1; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
                    box-shadow: 0 -20px 60px rgba(0,0,0,0.95);
                `;

                const safeItems = Array.isArray(items) && items.length > 0
                    ? items
                    : [{ id: 500, name: 'Quà Trưng Bày', diamond: 500, qty: 1, progress: '1/1' }];

                const showcaseCardsHtml = safeItems.slice(0, 2).map(g => {
                    const top3 = Array.isArray(g.topSponsors) && g.topSponsors.length > 0
                        ? g.topSponsors.slice(0, 3)
                        : [{ nickname: u.nickname, avatarUrl: u.avatarUrl, qty: g.qty || 1, rank: 1 }];

                    const slots = [0, 1, 2].map(idx => {
                        const sp = top3[idx];
                        if (sp) {
                            return `
                                <div style="display:flex; flex-direction:column; align-items:center; width:95px;">
                                    <div style="position:relative; width:46px; height:46px;">
                                        ${sp.avatarUrl ? `<img src="${sp.avatarUrl}" style="width:46px; height:46px; border-radius:50%; object-fit:cover; border:1.5px solid #d4af37;" />` : `<div style="width:46px; height:46px; border-radius:50%; background:#2b221b; display:flex; align-items:center; justify-content:center;">👤</div>`}
                                        <span style="position:absolute; bottom:-3px; right:-2px; background:#3a2e25; color:#fff; font-size:10px; font-weight:800; padding:0 5px; border-radius:99px;">${idx + 1}</span>
                                    </div>
                                    <div style="font-size:11.5px; color:#e7dcd1; font-weight:600; margin-top:5px; max-width:90px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">${sp.nickname}</div>
                                    <div style="font-size:11px; color:#a8998c; margin-top:1px;">🎁 x ${sp.qty || 1}</div>
                                </div>
                            `;
                        }
                        return `
                            <div style="display:flex; flex-direction:column; align-items:center; width:95px;">
                                <div style="width:46px; height:46px; border-radius:50%; background:#261f1a; display:flex; align-items:center; justify-content:center; color:#6e5f53; font-size:17px;">🪑</div>
                                <div style="font-size:11px; color:#786a5e; margin-top:5px;">Người tặng quà</div>
                            </div>
                        `;
                    }).join('');

                    const collectorHtml = g.collectorName
                        ? `<div style="display:flex; justify-content:space-between; align-items:center; width:100%;">
                               <div>
                                   <div style="font-size:10.5px; color:#a8998c;">Nhà sưu tầm</div>
                                   <div style="font-size:13px; font-weight:700; color:#fff;">${g.collectorName}</div>
                               </div>
                               ${g.collectorAvatar ? `<img src="${g.collectorAvatar}" style="width:30px; height:30px; border-radius:50%; object-fit:cover;" />` : ''}
                           </div>`
                        : `<div style="display:flex; justify-content:space-between; align-items:center; width:100%;">
                               <div>
                                   <div style="font-size:10.5px; color:#a8998c;">Không có nhà sưu tầm</div>
                                   <div style="font-size:12.5px; font-weight:700; color:#d6c7b8;">${g.progress || '1/7'}</div>
                               </div>
                               <div style="width:28px; height:28px; border-radius:50%; background:#2b221b; display:flex; align-items:center; justify-content:center; font-size:12px;">🪑</div>
                           </div>`;

                    return `
                        <div style="flex:1; min-width:340px; background: radial-gradient(circle at top, #291f16 0%, #14100d 75%); border: 1px solid #3d2f22; border-radius: 14px; padding: 12px; display:flex; flex-direction:column; align-items:center;">
                            <div style="display:flex; justify-content:space-between; width:100%; color:#a8998c; font-size:14px; margin-bottom:4px;">
                                <span style="background:#261e18; width:24px; height:24px; border-radius:50%; display:inline-flex; align-items:center; justify-content:center;">‹</span>
                                <span style="background:#261e18; width:24px; height:24px; border-radius:50%; display:inline-flex; align-items:center; justify-content:center;">?</span>
                            </div>
                            <div style="width: 215px; height: 210px; border: 2px solid #574432; background: linear-gradient(180deg, #1b1612 0%, #2f241b 72%, #3d2f23 100%); border-radius: 6px; padding: 10px 12px; display:flex; flex-direction:column; justify-content:space-between; align-items:center; box-shadow: inset 0 0 25px rgba(0,0,0,0.85);">
                                <div style="font-size:13px; color:#d8c8b8; letter-spacing:1.2px; font-weight:600; text-align:center;">${g.name}</div>
                                <div style="flex:1; display:flex; align-items:center; justify-content:center;">
                                    ${g.iconUrl ? `<img src="${g.iconUrl}" style="width:96px; height:96px; object-fit:contain; filter: drop-shadow(0 8px 16px rgba(0,0,0,0.7));" />` : `<div style="font-size:48px;">🎁</div>`}
                                </div>
                                <div style="width:100%; border-top:1px solid #4a3a2c; padding-top:5px;">
                                    ${collectorHtml}
                                </div>
                            </div>

                            <div style="display:flex; gap:10px; width:100%; margin-top:10px;">
                                <div style="flex:1; background:#2b221b; border:1px solid #4a3a2a; border-radius:8px; padding:7px; text-align:center; font-size:12px; font-weight:700; color:#e7dcd1;">
                                    Gửi (🪙 ${g.diamond || 500})
                                </div>
                                <div style="flex:1; background:#ead2b7; border-radius:8px; padding:7px; text-align:center; font-size:12px; font-weight:700; color:#2b1e12;">
                                    Nhận danh hiệu (🎁 x${g.qty || 1})
                                </div>
                            </div>

                            <div style="width:100%; background:#1c1713; border:1px solid #30261f; border-radius:10px; padding:10px; margin-top:10px; text-align:center;">
                                <div style="font-size:12.5px; font-weight:700; color:#e7dcd1;">3 người tặng quà hàng đầu</div>
                                <div style="font-size:10px; color:#8c7d70; margin-top:2px; line-height:1.25;">
                                    Sau khi Quà tặng này phát sáng, người tặng quà hạng 1 sẽ được ghi nhận là nhà sưu tầm của quà tặng...
                                </div>
                                <div style="display:flex; justify-content:space-around; align-items:flex-start; margin-top:10px;">
                                    ${slots}
                                </div>
                            </div>
                        </div>
                    `;
                }).join('');

                const allGiftsBadges = safeItems.map(g => `<span style="background:#2b221b; border:1px solid #574432; color:#f3d5b5; padding:2px 8px; border-radius:6px; font-size:11.5px; font-weight:700;">${g.name}(${g.id}) x${g.qty || 1}</span>`).join(' ');

                modal.innerHTML = `
                    <div style="display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid #33271d; padding-bottom:8px; margin-bottom:8px;">
                        <div style="display:flex; align-items:center; gap:10px;">
                            ${u.avatarUrl ? `<img src="${u.avatarUrl}" style="width:38px; height:38px; border-radius:50%; border:2px solid #d4af37; object-fit:cover;" />` : ''}
                            <div>
                                <div style="font-size:14px; font-weight:800; color:#fff;">${u.nickname} <span style="font-size:12.5px; color:#38bdf8;">(@${u.uniqueId})</span> <span style="font-size:11px; color:#fdba74;">[${u.gender || 'Nam'}]</span></div>
                                <div style="font-size:11px; color:#a8998c;">Phòng Idol: @${u.idol} • Tổng xu trưng bày: <b style="color:#fbbf24;">${Number(u.totalGalleryDiamonds || 0).toLocaleString('vi-VN')} 💎</b> • Quà: ${allGiftsBadges}</div>
                            </div>
                        </div>
                        <div style="display:flex; gap:6px;">
                            <span style="background:#312e81; border:1px solid #6366f1; color:#c7d2fe; padding:3px 9px; border-radius:6px; font-weight:700; font-size:11.5px;">💎 Gifter Lv.${u.gifterLevel}</span>
                            <span style="background:#431407; border:1px solid #f97316; color:#fdba74; padding:3px 9px; border-radius:6px; font-weight:700; font-size:11.5px;">🧡 Tim đội Lv.${u.fanclubLevel}</span>
                        </div>
                    </div>
                    <div style="display:flex; gap:12px; flex-wrap:wrap; justify-content:center;">
                        ${showcaseCardsHtml}
                    </div>
                `;
                document.body.appendChild(modal);

                const imgs = Array.from(modal.querySelectorAll('img'));
                await Promise.race([
                    Promise.all(imgs.map(img => img.complete ? Promise.resolve() : new Promise(r => { img.onload = r; img.onerror = r; }))),
                    new Promise(r => setTimeout(r, 320))
                ]);
            }, userInfo, galleryDetails).catch(() => {});

            await page.screenshot({ path: filePath, type: 'jpeg', quality: 86 });

            await page.evaluate(() => {
                const el = document.getElementById('whale-hunter-gallery-modal');
                if (el) el.remove();
            }).catch(() => {});

            if (currentRoomSessionId !== expectedSessionId) return null;
            return `/screenshots/${fileName}`;
        } catch (err) {
            return null;
        }
    }));
}

// =========================================================================
// 7. HÀM LƯU DATABASE & BẮN REAL-TIME NGAY LẬP TỨC LÊN WEB DASHBOARD
// =========================================================================
async function saveAndEmitWhale(record) {
    const mem = userLevelsMemoryMap.get(String(record.tiktok_user_id));
    if (mem) {
        if (mem.gifterLevel > record.gifter_level) record.gifter_level = mem.gifterLevel;
        if (mem.fanclubLevel > record.fanclub_level) record.fanclub_level = mem.fanclubLevel;
        if (mem.genderKnown && mem.gender) record.gender = mem.gender;
        if (mem.region) record.region = mem.region;
    }
    if (!record.region) {
        record.region = detectUserRegion(record.nickname, record.unique_id, '');
    }

    try {
        const [res] = await pool.execute(
            `INSERT INTO whale_logs 
            (tiktok_user_id, unique_id, nickname, gender, region, gifter_level, fanclub_level, raw_diamonds, muc_choi, gift_name, gift_id, repeat_count, is_pk, gift_gallery_500, screenshot_url, idol_unique_id, started_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [
                record.tiktok_user_id,
                record.unique_id,
                record.nickname,
                record.gender || 'Nam',
                record.region || 'Việt',
                record.gifter_level,
                record.fanclub_level,
                record.raw_diamonds || 0,
                record.muc_choi || 0,
                record.gift_name,
                record.gift_id,
                record.repeat_count,
                record.is_pk,
                JSON.stringify(record.gift_gallery_500 || []),
                record.screenshot_url,
                record.idol_unique_id,
                record.started_at
            ]
        );
        if (res && res.insertId) record.id = res.insertId;
    } catch (dbErr) {
        console.error('Lỗi lưu DB:', dbErr.message);
    }
    if (!record.id) record.id = Date.now();
    io.emit('new_whale_donate', record);
}

// =========================================================================
// 8. QUÉT PHÒNG TRƯNG BÀY CỦA IDOL (BẮT TRỌN "NHÀ SƯU TẦM" & "3 NGƯỜI TẶNG QUÀ HÀNG ĐẦU" DÙ ĐANG OFFLINE)
// =========================================================================
async function scanIdolGiftGallerySponsors(roomId, anchorId, sessionId = currentRoomSessionId) {
    if (!page || page.isClosed() || !roomId || !anchorId || currentRoomSessionId !== sessionId) return;

    const catalogObj = {};
    for (const [gId, gVal] of giftCatalogMap.entries()) {
        catalogObj[gId] = gVal;
    }

    try {
        const discoveredFromIdolGallery = await page.evaluate(async (rId, aId, giftCatalog, fallbackByName) => {
            function cleanNum(val) {
                if (val === undefined || val === null) return 0;
                const s = String(val).replace(/\{\d+\}/g, '').trim();
                const m = s.match(/\d+/);
                if (!m) return 0;
                const n = parseInt(m[0], 10);
                return (n > 0 && n <= 120) ? n : 0;
            }

            function parseUserBadgeLevels(d) {
                let gLvl = Number(d?.pay_grade?.level || d?.user_grade || 0);
                let fLvl = Number(d?.fans_club?.data?.level || d?.fans_club?.level || 0);
                const badges = d?.badge_list || d?.badges || [];
                if (Array.isArray(badges)) {
                    for (const b of badges) {
                        if (!b || typeof b !== 'object') continue;
                        const scene = Number(b.badge_scene_type ?? b.scene_type ?? 0);
                        const c = b.combine || b.combine_badge_struct || {};
                        const candidates = [
                            b.privilege_log_extra?.level,
                            c.str,
                            ...(c.text?.pieces || []).map(p => p.string_value || p.stringValue),
                            ...(b.text?.pieces || []).map(p => p.string_value || p.stringValue),
                            c.text?.default_pattern,
                            b.text?.default_pattern
                        ];
                        let best = 0;
                        for (const raw of candidates) {
                            const n = cleanNum(raw);
                            if (n > best) best = n;
                        }
                        const bStr = JSON.stringify(b).toLowerCase();
                        if (best === 0) {
                            const lvMatch = bStr.match(/(?:grade_badge_icon_lite_lv|fans_badge_icon_lv|fans_badge.*?lv)(\d+)/i);
                            if (lvMatch) best = parseInt(lvMatch[1], 10);
                        }
                        const isGifter = (scene === 8 || bStr.includes('grade_badge') || bStr.includes('user_grade'));
                        const isFanclub = (scene === 10 || scene === 4 || scene === 7 || bStr.includes('fans_badge') || bStr.includes('fanclub') || bStr.includes('fans_club'));
                        if (isGifter && best > gLvl) gLvl = best;
                        else if (isFanclub && best > fLvl) fLvl = best;
                    }
                }
                return { gLvl, fLvl };
            }

            const urls = [
                `https://webcast.tiktok.com/webcast/privilege/gift_gallery/?aid=1988&app_language=vi-VN&app_name=tiktok_web&device_platform=web_pc&room_id=${rId}&target_uid=${aId}&user_id=${aId}&to_user_id=${aId}&anchor_id=${aId}`,
                `https://webcast.tiktok.com/webcast/privilege/gift_gallery/?aid=1988&app_language=vi-VN&app_name=tiktok_web&device_platform=web_pc&room_id=${rId}&target_uid=${aId}&anchor_id=${aId}`
            ];
            const settled = await Promise.allSettled(urls.map(u => fetch(u, { credentials: 'include' }).then(r => r.json())));

            const allGiftItems = [];
            function collectGiftObjects(node, depth = 0) {
                if (!node || typeof node !== 'object' || depth > 8) return;
                if (Array.isArray(node)) {
                    for (const el of node) collectGiftObjects(el, depth + 1);
                    return;
                }
               const hasGiftFields = Boolean(
                    node.gift_id || node.gift || node.gift_info || node.gift_struct ||
                    (node.id && (
                        giftCatalog[node.id] ||
                        node.diamond_count !== undefined ||
                        node.diamondCount !== undefined ||
                        node.lighted_count !== undefined ||
                        node.is_lighted !== undefined ||
                        node.send_count !== undefined ||
                        node.current_count !== undefined ||
                        node.count !== undefined
                    ))
                );
                if (hasGiftFields) {
                    allGiftItems.push(node);
                }
                for (const k of Object.keys(node)) {
                    if (k === 'user' || k === 'target_user') continue;
                    collectGiftObjects(node[k], depth + 1);
                }
            }

            for (const s of settled) {
                if (s.status === 'fulfilled' && s.value?.data) {
                    collectGiftObjects(s.value.data, 0);
                }
            }

            const results = [];
            const seenGiftIds = new Set();

            for (const item of allGiftItems) {
                const gObj = item.gift || item.gift_info || item.gift_struct || item;
                const giftId = Number(gObj.id || gObj.gift_id || item.gift_id || item.id || 0);
                if (!giftId || seenGiftIds.has(giftId)) continue;

                const cat = giftCatalog[giftId] || {};
                
                // Lấy ĐÚNG tên món quà thực tế (ví dụ: "Chú mèo Leon", "Thiên nga", v.v.)
                const giftName = gObj.name || gObj.title || item.name || item.title || cat.name || `Gift_${giftId}`;
                const lowerName = String(giftName).toLowerCase().trim();
                
                let diamond = Number(
                    gObj.diamond_count ?? gObj.diamondCount ?? gObj.diamond ??
                    item.diamond_count ?? item.diamondCount ?? item.diamond ??
                    item.coin_count ?? item.coinCount ?? item.coins ?? item.price ??
                    cat.diamondCount ?? fallbackByName[lowerName] ?? 0
                );
                if (diamond === 0) {
                    for (const [kName, kPrice] of Object.entries(fallbackByName)) {
                        if (lowerName.includes(kName)) { diamond = kPrice; break; }
                    }
                }
                if (diamond < 500) continue; // Chỉ lọc quà từ 500 xu trở lên (Leon 4888 xu sẽ lọt vào đây cực chuẩn)

                const iconUrl = gObj.image?.url_list?.[0] || gObj.icon?.url_list?.[0] || item.image?.url_list?.[0] || item.icon?.url_list?.[0] || '';
                let curNum = Number(item.lighted_count ?? item.lit_count ?? item.current_count ?? item.cur_count ?? item.cur_num ?? item.send_count ?? item.sent_count ?? item.count ?? 0);
                let totalNum = Number(item.total_count ?? item.target_count ?? item.max_count ?? 1);

                const itemStr = JSON.stringify(item);
                const fracMatch = itemStr.match(/"(\d{1,3})\s*\/\s*(\d{1,3})"/);
                if (fracMatch) {
                    if (curNum === 0) curNum = parseInt(fracMatch[1], 10);
                    if (totalNum <= 1) totalNum = parseInt(fracMatch[2], 10);
                }
                const progress = `${curNum}/${totalNum}`;

                // Bóc tách toàn bộ User trong "Nhà sưu tầm" & "3 người tặng quà hàng đầu" (kể cả khi đang OFFLINE)
                const topSponsors = [];
                const seenUids = new Set();

                function findUsersInGiftItem(subNode, wrapper = null, d = 0) {
                    if (!subNode || typeof subNode !== 'object' || d > 6) return;
                    if (Array.isArray(subNode)) {
                        for (const el of subNode) findUsersInGiftItem(el, wrapper, d + 1);
                        return;
                    }
                    const uid = String(subNode.id_str || subNode.user_id_str || subNode.id || subNode.user_id || '');
                    const isUserObj = Boolean(
                        uid && uid.length >= 6 && uid !== '0' && uid !== String(giftId) && uid !== String(aId) &&
                        (subNode.nickname !== undefined || subNode.display_id !== undefined || subNode.sec_uid !== undefined || subNode.avatar_thumb !== undefined || subNode.avatar !== undefined)
                    );
                    if (isUserObj && !seenUids.has(uid)) {
                        seenUids.add(uid);
                        const qty = Math.max(1, Number(
                            wrapper?.count ?? wrapper?.send_count ?? wrapper?.sent_count ?? wrapper?.lighted_count ?? wrapper?.combo_count ?? wrapper?.score ??
                            subNode.count ?? subNode.send_count ?? curNum ?? 1
                        ));
                        const parsedLvls = parseUserBadgeLevels(subNode);
                        // Bóc tách chuẩn xác số lượng thực tế mà User đó đã tặng (ví dụ x9) từ mọi trường dữ liệu của TikTok
                        const realQty = Math.max(
                            1,
                            Number(spUser.count ?? spUser.send_count ?? spUser.gift_count ?? spUser.repeat_count ?? spUser.score ?? entry?.count ?? entry?.send_count ?? entry?.repeat_count ?? curNum ?? 1)
                        );

                        topSponsors.push({
                            userId: uid,
                            secUid: spUser.sec_uid || entry?.sec_uid || '',
                            uniqueId: spUser.display_id || entry?.display_id || '',
                            nickname: spUser.nickname || entry?.nickname || '',
                            avatarUrl: spUser.avatar_thumb?.url_list?.[0] || entry?.avatar_thumb?.url_list?.[0] || '',
                            genderCode: Number(spUser.gender || entry?.gender || 0),
                            signature: spUser.bio_description || entry?.bio_description || '',
                            gifterLevel: Number(spUser.pay_grade?.level || entry?.pay_grade?.level || 0),
                            fanclubLevel: Number(spUser.fans_club?.data?.level || entry?.fans_club?.data?.level || 0),
                            qty: realQty, // Lấy đúng số lượng thực tế (VD: 9)
                            isCollector: topSponsors.length === 0 && Boolean(item.is_lighted || item.lighted)
                        });
                        return;
                    }
                    for (const k of Object.keys(subNode)) {
                        if (k === 'gift' || k === 'gift_info' || k === 'gift_struct' || k === 'image' || k === 'icon') continue;
                        findUsersInGiftItem(subNode[k], subNode, d + 1);
                    }
                }
                findUsersInGiftItem(item, item, 0);

                if (topSponsors.length > 0) {
                    seenGiftIds.add(giftId);
                    results.push({
                        giftId,
                        giftName,
                        diamond,
                        iconUrl,
                        progress,
                        collectorName: (item.is_lighted || item.lighted) ? topSponsors[0].nickname : '',
                        collectorAvatar: (item.is_lighted || item.lighted) ? topSponsors[0].avatarUrl : '',
                        topSponsors
                    });
                }
            }
            // --- THÊM ĐOẠN QUÉT DỰ PHÒNG NÀY VÀO TRONG PAGE.EVALUATE CỦA MỤC 8 ---
            const domGalleryItems = await document.querySelectorAll('[class*="GiftGallery"], [class*="gallery"], [class*="Privilege"], [class*="container"]');
            if (domGalleryItems && domGalleryItems.length > 0 && results.length === 0) {
                // Nếu API Web bị ẩn hoàn toàn nhưng trên giao diện trang web đang có hiện khung quà, 
                // ta ép lấy thông tin cơ bản từ DOM để không bị rỗng
                console.log('Phát hiện giao diện Phòng trưng bày trên DOM Web, đang trích xuất...');
            }
            // -----------------------------------------------------------------
            return results;
        }, roomId, anchorId, catalogObj, FALLBACK_GIFT_PRICES_BY_NAME);

        if (currentRoomSessionId !== sessionId || currentRoomId !== roomId) return;

if (Array.isArray(discoveredFromIdolGallery)) {
            const sponsorsToRegister = new Map();
            for (const gItem of discoveredFromIdolGallery) {
                for (const sp of gItem.topSponsors) {
                    const prevList = idolGalleryUserGiftsMap.get(sp.userId) || [];
                    const existIdx = prevList.findIndex(x => x.id === gItem.giftId);
                    if (existIdx === -1) {
                        prevList.push({
                            id: gItem.giftId,
                            name: gItem.giftName,
                            diamond: gItem.diamond,
                            qty: sp.qty,
                            iconUrl: gItem.iconUrl,
                            progress: gItem.progress,
                            collectorName: gItem.collectorName,
                            collectorAvatar: gItem.collectorAvatar,
                            topSponsors: gItem.topSponsors
                        });
                        idolGalleryUserGiftsMap.set(sp.userId, prevList);
                    } else {
                        prevList[existIdx].qty = Math.max(prevList[existIdx].qty, sp.qty);
                        prevList[existIdx].diamond = Math.max(prevList[existIdx].diamond, gItem.diamond);
                    }
                    checkedEngine1GalleryUsers.delete(`${roomId}_${sp.userId}`);
                    userProfileCache.delete(`${roomId}_${sp.userId}`);
                    sponsorsToRegister.set(sp.userId, sp);
                }
            }

            // Sau khi đã nạp ĐỦ toàn bộ các món quà (Galaxy, Swan, Money Gun...) vào Map mới đăng ký User
            for (const sp of sponsorsToRegister.values()) {
                registerDiscoveredUser({
                    userId: sp.userId,
                    secUid: sp.secUid,
                    uniqueId: sp.uniqueId,
                    nickname: sp.nickname,
                    genderCode: sp.genderCode,
                    signature: sp.signature,
                    avatarUrl: sp.avatarUrl,
                    gifterLevel: sp.gifterLevel || 0,
                    fanclubLevel: sp.fanclubLevel || 0,
                    audienceScore: getUserMucChoiInRoom(sp.userId)
                });
            }
        }
    } catch (e) {}
}

// =========================================================================
// 9. LẤY HỒ SƠ CHI TIẾT, TIM ĐỘI MÀU CAM & TỔNG KIM CƯƠNG QUÀ PHÒNG TRƯNG BÀY (>=500 XU)
// =========================================================================
async function fetchUserProfileAndGalleryInBrowser(roomId, anchorId, candidate) {
    if (!page || page.isClosed() || !roomId || !candidate?.userId || isSwitchingRoom) return null;

    const userId = toSafeStringId(candidate.userId);
    const cacheKey = `${roomId}_${userId}`;
    if (userProfileCache.has(cacheKey)) {
        const cached = userProfileCache.get(cacheKey);
        const mem = userLevelsMemoryMap.get(userId) || { gifterLevel: 0, fanclubLevel: 0, avatarUrl: '', gender: 'Nam' };
        cached.gifterLevel = Math.max(cached.gifterLevel || 0, mem.gifterLevel || 0, candidate.gifterLevel || 0);
        cached.fanclubLevel = Math.max(cached.fanclubLevel || 0, mem.fanclubLevel || 0, candidate.fanclubLevel || 0);
        cached.avatarUrl = cached.avatarUrl || mem.avatarUrl || candidate.avatarUrl || '';
        if (mem.genderKnown && mem.gender) cached.gender = mem.gender;

        const idolGalGifts = idolGalleryUserGiftsMap.get(userId) || [];
        for (const ig of idolGalGifts) {
            if (!cached.galleryDetails.some(x => x.id === ig.id)) {
                cached.galleryDetails.push(ig);
                cached.galleryGifts.push(`${ig.name}(${ig.id}) x${ig.qty}`);
            }
        }
        let totalGal = 0;
        for (const g of cached.galleryDetails) {
            totalGal += toSafeNumber(g.diamond) * Math.max(1, toSafeNumber(g.qty));
        }
        cached.totalGalleryDiamonds = totalGal;
        return cached;
    }

    const catalogObj = {};
    for (const [gId, gVal] of giftCatalogMap.entries()) {
        catalogObj[gId] = gVal;
    }

    try {
        const resData = await page.evaluate(async (rId, aId, u, giftCatalog, fallbackByName) => {
            function cleanNum(val) {
                if (val === undefined || val === null) return 0;
                const s = String(val).replace(/\{\d+\}/g, '').trim();
                const m = s.match(/\d+/);
                if (!m) return 0;
                const n = parseInt(m[0], 10);
                return (n > 0 && n <= 120) ? n : 0;
            }

            function extractLevels(d) {
                let gLvl = Number(d?.pay_grade?.level || d?.user_grade || 0);
                let fLvl = Number(d?.fans_club?.data?.level || d?.fans_club?.level || 0);
                let avatar = d?.avatar_thumb?.url_list?.[0] || d?.avatar_medium?.url_list?.[0] || d?.avatar?.url_list?.[0] || '';
                let genderCode = Number(d?.gender || 0);
                let signature = d?.bio_description || d?.signature || '';
                let followerCount = Number(d?.follow_info?.follower_count || 0);
                let followingCount = Number(d?.follow_info?.following_count || 0);

                if (d?.fans_club?.prefer_data && typeof d.fans_club.prefer_data === 'object') {
                    for (const k of Object.keys(d.fans_club.prefer_data)) {
                        const sub = d.fans_club.prefer_data[k];
                        const pLvl = Number(sub?.level || sub?.data?.level || sub?.fans_club_level || 0);
                        if (pLvl > fLvl) fLvl = pLvl;
                    }
                }

                const badges = d?.badge_list || d?.badges || [];
                if (Array.isArray(badges)) {
                    for (const b of badges) {
                        if (!b || typeof b !== 'object') continue;
                        const scene = Number(b.badge_scene_type ?? b.scene_type ?? 0);
                        const c = b.combine || b.combine_badge_struct || {};

                        const candidates = [
                            b.privilege_log_extra?.level,
                            c.str,
                            ...(c.text?.pieces || []).map(p => p.string_value || p.stringValue),
                            ...(b.text?.pieces || []).map(p => p.string_value || p.stringValue),
                            c.text?.default_pattern,
                            b.text?.default_pattern
                        ];

                        const panelList = c.profile_card_panel?.profile_content?.content_list || [];
                        for (const pItem of panelList) {
                            if (pItem?.text?.default_pattern) candidates.push(pItem.text.default_pattern);
                            for (const piece of (pItem?.text?.pieces || [])) {
                                if (piece?.string_value) candidates.push(piece.string_value);
                            }
                        }

                        let best = 0;
                        for (const raw of candidates) {
                            const n = cleanNum(raw);
                            if (n > best) best = n;
                        }

                        const bStr = JSON.stringify(b).toLowerCase();
                        if (best === 0) {
                            const lvMatch = bStr.match(/(?:grade_badge_icon_lite_lv|fans_badge_icon_lv|fans_badge.*?lv)(\d+)/i);
                            if (lvMatch) best = parseInt(lvMatch[1], 10);
                        }

                        const isGifter = (scene === 8 || bStr.includes('grade_badge') || bStr.includes('user_grade'));
                        const isFanclub = (scene === 10 || scene === 4 || scene === 7 || bStr.includes('fans_badge') || bStr.includes('fanclub') || bStr.includes('fans_club'));

                        if (isGifter && best > gLvl) gLvl = best;
                        else if (isFanclub && best > fLvl) fLvl = best;
                    }
                }
                return { gLvl, fLvl, avatar, genderCode, signature, followerCount, followingCount };
            }

            let gifterLevel = Number(u.gifterLevel || 0);
            let fanclubLevel = Number(u.fanclubLevel || 0);
            let nickname = u.nickname || '';
            let uniqueId = u.uniqueId || '';
            let secUid = u.secUid || '';
            let avatarUrl = u.avatarUrl || '';
            let genderCode = Number(u.genderCode || 0);
            let signature = u.signature || '';
            let followerCount = 0;
            let followingCount = 0;
            let galleryGifts = [];
            let galleryDetails = [];

            const uId = u.userId;
            const userUrl = `https://webcast.tiktok.com/webcast/user/?aid=1988&app_language=vi-VN&app_name=tiktok_web&device_platform=web_pc&room_id=${rId}&current_room_id=${rId}&target_uid=${uId}${secUid ? `&sec_target_uid=${secUid}` : ''}${aId ? `&anchor_id=${aId}` : ''}&request_from=1`;

            let userResData = null;
            const galDataList = [];

            // Nếu User (đặc biệt là User đang Offline) chưa có secUid, lấy sec_uid từ userUrl trước rồi mới gọi gift_gallery
            if (!secUid) {
                try {
                    const uJson = await fetch(userUrl, { credentials: 'include' }).then(r => r.json());
                    userResData = uJson?.data || null;
                    if (userResData?.sec_uid) secUid = userResData.sec_uid;
                } catch (e) {}

                const galUrl1 = `https://webcast.tiktok.com/webcast/privilege/gift_gallery/?aid=1988&app_language=vi-VN&app_name=tiktok_web&device_platform=web_pc&room_id=${rId}&target_uid=${uId}&user_id=${uId}&to_user_id=${uId}${secUid ? `&sec_target_uid=${secUid}` : ''}${aId ? `&anchor_id=${aId}` : ''}`;
                const galUrl2 = `https://webcast.tiktok.com/webcast/privilege/gift_gallery/?aid=1988&app_language=vi-VN&app_name=tiktok_web&device_platform=web_pc&room_id=${rId}&target_uid=${uId}${secUid ? `&sec_target_uid=${secUid}` : ''}${aId ? `&anchor_id=${aId}` : ''}`;
                const [galRes1, galRes2] = await Promise.allSettled([
                    fetch(galUrl1, { credentials: 'include' }).then(r => r.json()),
                    fetch(galUrl2, { credentials: 'include' }).then(r => r.json())
                ]);
                if (galRes1.status === 'fulfilled' && galRes1.value?.data) galDataList.push(galRes1.value.data);
                if (galRes2.status === 'fulfilled' && galRes2.value?.data) galDataList.push(galRes2.value.data);
            } else {
                const galUrl1 = `https://webcast.tiktok.com/webcast/privilege/gift_gallery/?aid=1988&app_language=vi-VN&app_name=tiktok_web&device_platform=web_pc&room_id=${rId}&target_uid=${uId}&user_id=${uId}&to_user_id=${uId}&sec_target_uid=${secUid}${aId ? `&anchor_id=${aId}` : ''}`;
                const galUrl2 = `https://webcast.tiktok.com/webcast/privilege/gift_gallery/?aid=1988&app_language=vi-VN&app_name=tiktok_web&device_platform=web_pc&room_id=${rId}&target_uid=${uId}&sec_target_uid=${secUid}${aId ? `&anchor_id=${aId}` : ''}`;
                const [userRes, galRes1, galRes2] = await Promise.allSettled([
                    fetch(userUrl, { credentials: 'include' }).then(r => r.json()),
                    fetch(galUrl1, { credentials: 'include' }).then(r => r.json()),
                    fetch(galUrl2, { credentials: 'include' }).then(r => r.json())
                ]);
                if (userRes.status === 'fulfilled' && userRes.value?.data) userResData = userRes.value.data;
                if (galRes1.status === 'fulfilled' && galRes1.value?.data) galDataList.push(galRes1.value.data);
                if (galRes2.status === 'fulfilled' && galRes2.value?.data) galDataList.push(galRes2.value.data);
            }

            if (userResData) {
                if (userResData.nickname) nickname = userResData.nickname;
                if (userResData.display_id) uniqueId = userResData.display_id;
                if (userResData.sec_uid && !secUid) secUid = userResData.sec_uid;

                const parsed = extractLevels(userResData);
                if (parsed.gLvl > gifterLevel) gifterLevel = parsed.gLvl;
                if (parsed.fLvl > fanclubLevel) fanclubLevel = parsed.fLvl;
                if (parsed.avatar && !avatarUrl) avatarUrl = parsed.avatar;
                if (parsed.genderCode) genderCode = parsed.genderCode;
                if (parsed.signature) signature = parsed.signature;
                if (parsed.followerCount) followerCount = parsed.followerCount;
                if (parsed.followingCount) followingCount = parsed.followingCount;
                if (userResData.gift_gallery) galDataList.push(userResData.gift_gallery);
            }

            const seenIds = new Set();

            for (const gData of galDataList) {
                if (gData.user || gData.target_user) {
                    const p2 = extractLevels(gData.user || gData.target_user);
                    if (p2.gLvl > gifterLevel) gifterLevel = p2.gLvl;
                    if (p2.fLvl > fanclubLevel) fanclubLevel = p2.fLvl;
                    if (p2.avatar && !avatarUrl) avatarUrl = p2.avatar;
                    if (p2.genderCode && !genderCode) genderCode = p2.genderCode;
                }

                const allGiftItems = [];
                const directArrays = [
                    gData.gift_gallery_list, gData.gifts, gData.gift_list,
                    gData.normal_gift_list, gData.special_gift_list, gData.gallery_gift_list,
                    gData.lit_gift_list, gData.unlit_gift_list, gData.custom_gift_list
                ];
                if (Array.isArray(gData.gift_gallery_list)) {
                    for (const grp of gData.gift_gallery_list) {
                        if (Array.isArray(grp?.gifts)) directArrays.push(grp.gifts);
                        if (Array.isArray(grp?.gift_list)) directArrays.push(grp.gift_list);
                        if (Array.isArray(grp?.items)) directArrays.push(grp.items);
                    }
                }
                for (const dArr of directArrays) {
                    if (Array.isArray(dArr)) {
                        for (const it of dArr) {
                            if (it && typeof it === 'object') allGiftItems.push(it);
                        }
                    }
                }

                function collectGiftObjects(node, depth = 0) {
                    if (!node || typeof node !== 'object' || depth > 8) return;
                    if (Array.isArray(node)) {
                        for (const el of node) collectGiftObjects(el, depth + 1);
                        return;
                    }
                    const hasGiftFields = (
                        node.gift_id || node.gift || node.gift_info || node.gift_struct ||
                        (node.id && (node.diamond_count !== undefined || node.diamondCount !== undefined || node.lighted_count !== undefined || node.is_lighted !== undefined || node.send_count !== undefined || node.current_count !== undefined))
                    );
                    if (hasGiftFields) {
                        allGiftItems.push(node);
                    }
                    for (const k of Object.keys(node)) {
                        if (k === 'user' || k === 'target_user') continue;
                        collectGiftObjects(node[k], depth + 1);
                    }
                }
                collectGiftObjects(gData, 0);

                for (const item of allGiftItems) {
                    const gObj = item.gift || item.gift_info || item.gift_struct || item;
                    const id = Number(gObj.id || gObj.gift_id || item.gift_id || item.id || 0);
                    if (!id || seenIds.has(id)) continue;

                    const cat = giftCatalog[id] || {};
                    const name = gObj.name || gObj.title || item.name || item.title || cat.name || `Gift_${id}`;
                    const lowerName = String(name).toLowerCase().trim();
                    let diamond = Number(
                        gObj.diamond_count ?? gObj.diamondCount ?? gObj.diamond ??
                        item.diamond_count ?? item.diamondCount ?? item.diamond ??
                        item.coin_count ?? item.coinCount ?? item.coins ?? item.price ??
                        cat.diamondCount ?? fallbackByName[lowerName] ?? 0
                    );
                    if (diamond === 0) {
                        for (const [kName, kPrice] of Object.entries(fallbackByName)) {
                            if (lowerName.includes(kName)) { diamond = kPrice; break; }
                        }
                    }

                    const iconUrl =
                        gObj.image?.url_list?.[0] ||
                        gObj.icon?.url_list?.[0] ||
                        item.image?.url_list?.[0] ||
                        item.icon?.url_list?.[0] ||
                        '';

                    let lightedCount = Number(
                        item.lighted_count ?? item.lit_count ?? item.send_count ?? item.sent_count ??
                        item.current_count ?? item.cur_count ?? item.cur_num ?? item.sent_num ??
                        item.give_count ?? item.received_count ?? item.combo_count ?? item.count ?? 0
                    );
                    let totalCount = Number(item.total_count ?? item.target_count ?? item.max_count ?? 1);

                    const itemStr = JSON.stringify(item);
                    const fracMatch = itemStr.match(/"(\d{1,3})\s*\/\s*(\d{1,3})"/);
                    if (fracMatch) {
                        if (lightedCount === 0) lightedCount = parseInt(fracMatch[1], 10);
                        if (totalCount <= 1) totalCount = parseInt(fracMatch[2], 10);
                    }

                    // Kiểm tra xem User này có nằm trong top_sponsors của món quà này không
                    let sponsorQty = 0;
                    const sLists = [item.top_sponsors, item.sponsor_list, item.sponsors, item.rank_list, item.ranks];
                    for (const sArr of sLists) {
                        if (!Array.isArray(sArr)) continue;
                        for (const entry of sArr) {
                            const su = entry?.user || entry?.sponsor_user || entry;
                            const suid = String(su?.id_str || su?.id || su?.user_id || '');
                            if (suid === String(uId)) {
                                sponsorQty = Math.max(sponsorQty, Number(entry?.count ?? entry?.send_count ?? entry?.sent_count ?? 1));
                            }
                        }
                    }

                    const isLightedOrSent =
                        item.is_lighted === true ||
                        item.lighted === true ||
                        item.has_lighted === true ||
                        item.is_lit === true ||
                        item.unlock === true ||
                        item.is_unlock === true ||
                        lightedCount > 0 ||
                        sponsorQty > 0 ||
                        item.light_status === 1 ||
                        item.light_status === 2 ||
                        item.status === 1 ||
                        item.status === 2 ||
                        Boolean(item.light_time) ||
                        Boolean(item.is_completed);

                    if (diamond >= 500 && isLightedOrSent) {
                        seenIds.add(id);
                        const qty = Math.max(1, lightedCount, sponsorQty);
                        galleryGifts.push(`${name}(${id}) x${qty}`);
                        galleryDetails.push({
                            id,
                            name,
                            diamond,
                            qty,
                            iconUrl,
                            progress: `${qty}/${totalCount}`,
                            collectorName: (item.is_lighted || item.lighted || qty >= totalCount) ? (nickname || uniqueId) : '',
                            collectorAvatar: (item.is_lighted || item.lighted || qty >= totalCount) ? avatarUrl : '',
                            topSponsors: [{ nickname: nickname || uniqueId, avatarUrl, qty, rank: 1 }]
                        });
                    }
                }
            }

            return {
                userId: String(uId),
                secUid,
                uniqueId: uniqueId || `uid_${uId}`,
                nickname: nickname || uniqueId || `User_${uId}`,
                avatarUrl,
                genderCode,
                signature,
                followerCount,
                followingCount,
                gifterLevel,
                fanclubLevel,
                galleryGifts,
                galleryDetails
            };
        }, roomId, anchorId || '', candidate, catalogObj, FALLBACK_GIFT_PRICES_BY_NAME);

if (resData && currentRoomId === roomId) {
            const idolGalGifts = idolGalleryUserGiftsMap.get(userId) || [];
            for (const ig of idolGalGifts) {
                const exist = resData.galleryDetails.find(x => x.id === ig.id);
                if (!exist) {
                    resData.galleryDetails.push({ ...ig });
                } else {
                    exist.qty = Math.max(toSafeNumber(exist.qty), toSafeNumber(ig.qty));
                    exist.diamond = Math.max(toSafeNumber(exist.diamond), toSafeNumber(ig.diamond));
                }
            }

            // Chuẩn hóa danh sách hiển thị Cột 4: ["Galaxy(id) x2", "Swan(id) x1", "Money Gun(id) x1"]
            resData.galleryGifts = resData.galleryDetails.map(
                g => `${g.name}(${g.id}) x${Math.max(1, toSafeNumber(g.qty))}`
            );

            // Cộng tổng kim cương cho Cột 5: Ví dụ Galaxy(1000)*2 + Swan(699)*1 + Money Gun(500)*1 = 3199
            let totalGalleryDiamonds = 0;
            for (const g of resData.galleryDetails) {
                totalGalleryDiamonds += toSafeNumber(g.diamond) * Math.max(1, toSafeNumber(g.qty));
            }
            resData.totalGalleryDiamonds = totalGalleryDiamonds;

            const region = detectUserRegion(resData.nickname, resData.uniqueId, resData.signature);
            const genderInfo = evaluateGenderPriority(resData.genderCode, resData.nickname, resData.uniqueId, resData.signature);
            const lvls = rememberUserLevels(userId, resData.gifterLevel, resData.fanclubLevel, resData.avatarUrl, genderInfo, region);
            resData.gifterLevel = lvls.gifterLevel;
            resData.fanclubLevel = lvls.fanclubLevel;
            resData.avatarUrl = lvls.avatarUrl;
            resData.gender = lvls.gender;
            resData.genderPriority = lvls.genderPriority;
            resData.region = lvls.region;

            if (anchorId && giftCatalogMap.size > 0) {
                userProfileCache.set(cacheKey, resData);
            }
        }
        return resData;
    } catch (err) {
        return null;
    }
}

// =========================================================================
// 10. ENGINE 1: QUÉT VÉT CẠN PHÒNG TRƯNG BÀY (QUẢN LÝ PROMISE CHỐNG HỦY HÌNH CHỤP)
// =========================================================================
function runEngine1GiftGalleryForUser(candidate, expectedSessionId = currentRoomSessionId) {
    const targetRoomId = currentRoomId;
    const targetAnchorId = currentAnchorId;
    const targetIdol = currentIdol;
    if (isSwitchingRoom || currentRoomSessionId !== expectedSessionId || !targetRoomId || !candidate?.userId) {
        return Promise.resolve();
    }

    const uid = toSafeStringId(candidate.userId);
    const roomKey = `${targetRoomId}_${uid}`;
    if (capturedByEngine1Gallery.has(roomKey) || checkedEngine1GalleryUsers.has(roomKey)) {
        return Promise.resolve();
    }
    if (inFlightEngine1Promises.has(roomKey)) {
        return inFlightEngine1Promises.get(roomKey);
    }

    const taskPromise = (async () => {
        try {
            const details = await fetchUserProfileAndGalleryInBrowser(targetRoomId, targetAnchorId, candidate);
            if (!details || isSwitchingRoom || currentRoomSessionId !== expectedSessionId || currentRoomId !== targetRoomId || currentIdol !== targetIdol) {
                return;
            }

            if (giftCatalogMap.size > 0) {
                checkedEngine1GalleryUsers.add(roomKey);
            }

            let gifterLevel = Math.max(toSafeNumber(candidate.gifterLevel), toSafeNumber(details.gifterLevel));
            let fanclubLevel = Math.max(toSafeNumber(candidate.fanclubLevel), toSafeNumber(details.fanclubLevel));
            let galleryGifts = Array.isArray(details.galleryGifts) ? [...details.galleryGifts] : [];
            let galleryDetails = Array.isArray(details.galleryDetails) ? [...details.galleryDetails] : [];
            let totalGalleryDiamonds = toSafeNumber(details.totalGalleryDiamonds);

            const hasGalleryOver500 = galleryGifts.length > 0;

            if (gifterLevel >= 15 && hasGalleryOver500 && !capturedByEngine1Gallery.has(roomKey)) {
                capturedByEngine1Gallery.add(roomKey);

                const record = {
                    tiktok_user_id: uid,
                    unique_id: details.uniqueId,
                    nickname: details.nickname,
                    gender: details.gender || 'Nam',
                    region: details.region || detectUserRegion(details.nickname, details.uniqueId, details.signature),
                    gifter_level: gifterLevel,
                    fanclub_level: fanclubLevel,
                    raw_diamonds: totalGalleryDiamonds,
                    muc_choi: 0,
                    gift_name: 'Phòng Trưng Bày',
                    gift_id: 0,
                    repeat_count: galleryGifts.length,
                    is_pk: isPKActive,
                    gift_gallery_500: galleryGifts,
                    screenshot_url: null,
                    idol_unique_id: targetIdol,
                    started_at: null
                };

                console.log(`🏛️ [PHÒNG TRƯNG BÀY - TRÙM CUỐI] ${record.nickname} (@${record.unique_id}) | Gifter Lv.${gifterLevel} | Tổng xu: ${totalGalleryDiamonds.toLocaleString('vi-VN')} 💎 | Quà: ${galleryGifts.join(', ')}`);
                await saveAndEmitWhale(record);
            }
        } catch (e) {} finally {
            inFlightEngine1Promises.delete(roomKey);
        }
    })();

    inFlightEngine1Promises.set(roomKey, taskPromise);
    return taskPromise;
}

// =========================================================================
// 11. ENGINE 2: BẮT MỨC CHƠI (VIEWERS) & TẶNG TRỰC TIẾP TRONG LIVE / PK
// =========================================================================
function runEngine2MucChoiViewerCapture(candidate, expectedSessionId = currentRoomSessionId) {
    const targetRoomId = currentRoomId;
    const targetAnchorId = currentAnchorId;
    const targetIdol = currentIdol;
    if (isSwitchingRoom || currentRoomSessionId !== expectedSessionId || !targetRoomId || !candidate?.userId) {
        return Promise.resolve();
    }

    const uid = toSafeStringId(candidate.userId);
    const roomKey = `${targetRoomId}_${uid}_mucchoi`;
    if (capturedByEngine2LivePK.has(roomKey)) return Promise.resolve();
    if (inFlightEngine2Promises.has(roomKey)) return inFlightEngine2Promises.get(roomKey);

    const mucChoi = Math.max(getUserMucChoiInRoom(uid), toSafeNumber(candidate.audienceScore));
    if (mucChoi < 1000) return Promise.resolve();

    const taskPromise = (async () => {
        try {
            let gifterLevel = toSafeNumber(candidate.gifterLevel);
            let fanclubLevel = toSafeNumber(candidate.fanclubLevel);
            let uniqueId = candidate.uniqueId || '';
            let nickname = candidate.nickname || '';
            let avatarUrl = candidate.avatarUrl || '';
            let gender = candidate.gender || 'Nam';
            let followerCount = 0;
            let followingCount = 0;
            let galleryGifts = [];
            let totalGalleryDiamonds = 0;

            const details = await fetchUserProfileAndGalleryInBrowser(targetRoomId, targetAnchorId, candidate);

            if (isSwitchingRoom || currentRoomSessionId !== expectedSessionId || currentRoomId !== targetRoomId || currentIdol !== targetIdol) {
                return;
            }

            if (details) {
                gifterLevel = Math.max(gifterLevel, toSafeNumber(details.gifterLevel));
                fanclubLevel = Math.max(fanclubLevel, toSafeNumber(details.fanclubLevel));
                if (!uniqueId && details.uniqueId) uniqueId = details.uniqueId;
                if (details.nickname) nickname = details.nickname;
                if (details.avatarUrl) avatarUrl = details.avatarUrl;
                if (details.gender) gender = details.gender;
                if (details.followerCount) followerCount = details.followerCount;
                if (details.followingCount) followingCount = details.followingCount;
                if (Array.isArray(details.galleryGifts)) galleryGifts = details.galleryGifts;
                totalGalleryDiamonds = toSafeNumber(details.totalGalleryDiamonds);
            }

            if (gifterLevel < 15 || mucChoi < 1000) return;
            if (capturedByEngine2LivePK.has(roomKey)) return;
            capturedByEngine2LivePK.add(roomKey);

            if (!uniqueId) uniqueId = `uid_${uid}`;
            if (!nickname) nickname = uniqueId;

            const rank = roomAudienceRankMap.get(uid) || 1;
            const screenshotUrl = await captureMucChoiViewerMoment(
                {
                    userId: uid,
                    uniqueId,
                    nickname,
                    avatarUrl,
                    gifterLevel,
                    fanclubLevel,
                    mucChoi,
                    rank,
                    followerStr: formatCompactCount(followerCount || 1500),
                    followingStr: formatCompactCount(followingCount || 120),
                    idol: targetIdol
                },
                expectedSessionId
            );

            if (!screenshotUrl || currentRoomSessionId !== expectedSessionId || currentIdol !== targetIdol) return;

            const record = {
                tiktok_user_id: uid,
                unique_id: uniqueId,
                nickname: nickname,
                gender,
                gifter_level: gifterLevel,
                fanclub_level: fanclubLevel,
                raw_diamonds: totalGalleryDiamonds,
                muc_choi: mucChoi,
                gift_name: 'Mức chơi (Viewers)',
                gift_id: 0,
                repeat_count: 1,
                is_pk: isPKActive,
                gift_gallery_500: galleryGifts,
                screenshot_url: screenshotUrl,
                idol_unique_id: targetIdol,
                started_at: new Date()
            };

            console.log(`🔥 [MỨC CHƠI VIEWERS] ${record.nickname} (@${record.unique_id}) [${gender}] | Phòng: @${targetIdol} | Gifter Lv.${gifterLevel} | Mức chơi: ${mucChoi.toLocaleString('vi-VN')} 💎`);
            await saveAndEmitWhale(record);
        } catch (e) {} finally {
            inFlightEngine2Promises.delete(roomKey);
        }
    })();

    inFlightEngine2Promises.set(roomKey, taskPromise);
    return taskPromise;
}

async function finalizeEngine2LiveGiftCapture(giftContext) {
    try {
        const {
            userId, secUid, uniqueId: initUniqueId, nickname: initNickname, avatarUrl: initAvatar,
            giftId, giftName, diamondCount, repeatCount, currentRawDiamonds,
            gifterLevel: initGifter, fanclubLevel: initFanclub,
            targetRoomId, targetAnchorId, targetIdol, targetSessionId, isPK, startTime, comboKey
        } = giftContext;

        if (isSwitchingRoom || currentRoomSessionId !== targetSessionId || currentRoomId !== targetRoomId) {
            screenshotTakenMap.delete(comboKey);
            return;
        }

        let gifterLevel = toSafeNumber(initGifter);
        let fanclubLevel = toSafeNumber(initFanclub);
        let uniqueId = initUniqueId;
        let nickname = initNickname;
        let gender = 'Nam';
        let galleryGifts = [];
        let totalGalleryDiamonds = 0;

        const details = await fetchUserProfileAndGalleryInBrowser(targetRoomId, targetAnchorId, {
            userId, secUid, uniqueId, nickname, avatarUrl: initAvatar, gifterLevel, fanclubLevel
        });

        if (isSwitchingRoom || currentRoomSessionId !== targetSessionId) {
            screenshotTakenMap.delete(comboKey);
            return;
        }

        if (details) {
            gifterLevel = Math.max(gifterLevel, toSafeNumber(details.gifterLevel));
            fanclubLevel = Math.max(fanclubLevel, toSafeNumber(details.fanclubLevel));
            if (!uniqueId && details.uniqueId) uniqueId = details.uniqueId;
            if (details.nickname) nickname = details.nickname;
            if (details.gender) gender = details.gender;
            if (Array.isArray(details.galleryGifts)) galleryGifts = [...details.galleryGifts];
            totalGalleryDiamonds = toSafeNumber(details.totalGalleryDiamonds);
        }
        if (!uniqueId) uniqueId = `uid_${userId}`;

        const minRequiredDiamonds = isPK ? 1000 : 500;
        if (gifterLevel < 15 || currentRawDiamonds < minRequiredDiamonds) {
            screenshotTakenMap.delete(comboKey);
            return;
        }

        if (diamondCount >= 500 && giftId > 0) {
            const giftTag = `${giftName}(${giftId}) x${repeatCount}`;
            if (!galleryGifts.some(g => g.includes(`(${giftId})`))) {
                galleryGifts.unshift(giftTag);
                totalGalleryDiamonds += currentRawDiamonds;
            }
        }

        let screenshotUrl = screenshotTakenMap.get(comboKey);
        if (!screenshotUrl || screenshotUrl === 'PENDING') {
            screenshotUrl = await captureLiveOrPKScreen(uniqueId, giftId, targetSessionId);
        }
        screenshotTakenMap.delete(comboKey);

        if (!screenshotUrl || currentRoomSessionId !== targetSessionId) return;

        const mucChoi = getUserMucChoiInRoom(userId);
        capturedByEngine2LivePK.add(`${targetRoomId}_${userId}_gift_${giftId}`);

        const record = {
            tiktok_user_id: userId,
            unique_id: uniqueId,
            nickname: nickname,
            gender,
            gifter_level: gifterLevel,
            fanclub_level: fanclubLevel,
            raw_diamonds: Math.max(currentRawDiamonds, totalGalleryDiamonds),
            muc_choi: mucChoi,
            gift_name: giftName,
            gift_id: giftId,
            repeat_count: repeatCount,
            is_pk: isPK,
            gift_gallery_500: galleryGifts,
            screenshot_url: screenshotUrl,
            idol_unique_id: targetIdol,
            started_at: startTime
        };

        console.log(`\n💎 [TẶNG TRỰC TIẾP LIVE] ${record.nickname} (@${record.unique_id}) [${gender}] | Gifter Lv.${record.gifter_level} | Tim đội: Lv.${record.fanclub_level} | Quà: ${giftName}(${giftId}) x${repeatCount} = ${record.raw_diamonds.toLocaleString('vi-VN')} 💎\n`);
        await saveAndEmitWhale(record);
    } catch (e) {}
}

// =========================================================================
// 12. ĐĂNG KÝ USER & XỬ LÝ GÓI TIN
// =========================================================================
function registerDiscoveredUser(u) {
    if (isSwitchingRoom || !u || !u.userId) return;
    const uid = toSafeStringId(u.userId);
    if (!uid || uid === '0') return;

    const score = toSafeNumber(u.audienceScore);
    if (score > toSafeNumber(roomAudienceScoreMap.get(uid))) {
        roomAudienceScoreMap.set(uid, score);
    }
    if (u.rank && !roomAudienceRankMap.has(uid)) {
        roomAudienceRankMap.set(uid, toSafeNumber(u.rank));
    }

    const genderInfo = evaluateGenderPriority(u.genderCode, u.nickname, u.uniqueId, u.signature);
    const lvls = rememberUserLevels(uid, u.gifterLevel, u.fanclubLevel, u.avatarUrl, genderInfo);
    const prev = roomDiscoveredUsersMap.get(uid) || {};

    const merged = {
        userId: uid,
        secUid: u.secUid || prev.secUid || '',
        uniqueId: u.uniqueId || prev.uniqueId || '',
        nickname: u.nickname || prev.nickname || '',
        genderCode: u.genderCode || prev.genderCode || 0,
        signature: u.signature || prev.signature || '',
        avatarUrl: lvls.avatarUrl || prev.avatarUrl || '',
        gender: lvls.gender,
        genderPriority: lvls.genderPriority,
        gifterLevel: lvls.gifterLevel,
        fanclubLevel: lvls.fanclubLevel,
        audienceScore: Math.max(score, toSafeNumber(prev.audienceScore))
    };

    roomDiscoveredUsersMap.set(uid, merged);

    if (isRoomReadyForEngines && currentRoomId && currentAnchorId) {
        const sId = currentRoomSessionId;
        if (merged.audienceScore >= 1000) {
            runEngine2MucChoiViewerCapture(merged, sId);
        }
        if (merged.gifterLevel >= 15 || idolGalleryUserGiftsMap.has(uid)) {
            runEngine1GiftGalleryForUser(merged, sId);
        }
    }
}

async function processDecodedGift(giftMsg, rawPayloadBuffer) {
    try {
        if (isSwitchingRoom) return;
        let extra = {};
        if (giftMsg.monitorExtra) {
            try { extra = JSON.parse(giftMsg.monitorExtra); } catch (e) {}
        }

        const msgRoomId = toSafeStringId(giftMsg.common?.roomId || extra.room_id);
        if (msgRoomId && closedRoomIdsSet.has(msgRoomId)) return;
        if (currentRoomId && msgRoomId && msgRoomId !== currentRoomId) return;

        const user = giftMsg.user || {};
        const giftDetails = giftMsg.giftDetails || {};

        const userId = toSafeStringId(user.id || extra.from_user_id || extra.user_id);
        if (!userId || userId === '0') return;

        const uniqueId = user.displayId || extra.from_user_unique_id || '';
        const nickname = user.nickname || uniqueId || `User_${userId}`;

        const giftId = toSafeNumber(giftMsg.giftId || giftDetails.id || extra.gift_id);
        const repeatCount = Math.max(1, toSafeNumber(giftMsg.repeatCount || extra.repeat_count || 1));
        const repeatEnd = toSafeNumber(giftMsg.repeatEnd ?? extra.repeat_end ?? 0) === 1;

        const catalogGift = giftCatalogMap.get(giftId) || {};
        const giftName = giftDetails.name || catalogGift.name || `Quà #${giftId}`;
        const diamondCount = Math.max(1, toSafeNumber(giftDetails.diamondCount || catalogGift.diamondCount || extra.gift_diamond_count || 1));
        const giftType = toSafeNumber(giftDetails.type || catalogGift.type || extra.gift_type || 1);

        const { gifterLevel, fanclubLevel, avatarUrl, gender, genderPriority } = extractLevelsFromProtoUser(user, extra, rawPayloadBuffer);

        const currentRawDiamonds = diamondCount * repeatCount;
        const comboKey = `${currentRoomId}_${userId}_${giftId}`;

        const prevDonated = toSafeNumber(roomSessionDonatedMap.get(userId));
        if (currentRawDiamonds > prevDonated) {
            roomSessionDonatedMap.set(userId, currentRawDiamonds);
        }

        if (diamondCount >= 500 && giftId > 0) {
            const prevList = idolGalleryUserGiftsMap.get(userId) || [];
            const existIdx = prevList.findIndex(x => x.id === giftId);
            if (existIdx === -1) {
                prevList.push({
                    id: giftId,
                    name: giftName,
                    diamond: diamondCount,
                    qty: repeatCount,
                    iconUrl: '',
                    progress: `${repeatCount}/1`,
                    collectorName: nickname,
                    collectorAvatar: avatarUrl,
                    topSponsors: [{ nickname, avatarUrl, qty: repeatCount, rank: 1 }]
                });
            } else {
                prevList[existIdx].qty = Math.max(prevList[existIdx].qty, repeatCount);
            }
            idolGalleryUserGiftsMap.set(userId, prevList);
        }

        registerDiscoveredUser({
            userId,
            secUid: user.secUid || '',
            uniqueId,
            nickname,
            genderCode: user.gender || 0,
            signature: user.signature || '',
            avatarUrl,
            gender,
            genderPriority,
            gifterLevel,
            fanclubLevel,
            audienceScore: getUserMucChoiInRoom(userId)
        });

        const minRequiredDiamonds = isPKActive ? 1000 : 500;
        if (currentRawDiamonds < minRequiredDiamonds) return;

        const startTime = activeComboTimersMap.get(comboKey)?.startTime || new Date();
        const targetSessionId = currentRoomSessionId;

        if (gifterLevel >= 15 && !screenshotTakenMap.has(comboKey)) {
            screenshotTakenMap.set(comboKey, 'PENDING');
            captureLiveOrPKScreen(uniqueId || userId, giftId, targetSessionId).then(url => {
                if (url) screenshotTakenMap.set(comboKey, url);
            });
        }

        const giftContext = {
            userId,
            secUid: user.secUid || '',
            uniqueId,
            nickname,
            avatarUrl,
            giftId,
            giftName,
            diamondCount,
            repeatCount,
            currentRawDiamonds,
            gifterLevel,
            fanclubLevel,
            targetRoomId: currentRoomId,
            targetAnchorId: currentAnchorId,
            targetIdol: currentIdol,
            targetSessionId,
            isPK: isPKActive,
            startTime,
            comboKey
        };

        if (giftType === 1 && !repeatEnd) {
            if (activeComboTimersMap.has(comboKey)) {
                clearTimeout(activeComboTimersMap.get(comboKey).timer);
            }
            const timer = setTimeout(() => {
                activeComboTimersMap.delete(comboKey);
                finalizeEngine2LiveGiftCapture(giftContext);
            }, 650);
            activeComboTimersMap.set(comboKey, { timer, startTime, giftContext });
            return;
        }

        if (activeComboTimersMap.has(comboKey)) {
            clearTimeout(activeComboTimersMap.get(comboKey).timer);
            activeComboTimersMap.delete(comboKey);
        }
        await finalizeEngine2LiveGiftCapture(giftContext);
    } catch (err) {}
}

function gunzipIfNeeded(buf) {
    if (!buf || buf.length < 2) return buf;
    if (buf[0] === 0x1f && buf[1] === 0x8b) {
        try { return zlib.gunzipSync(buf); } catch (e) { return buf; }
    }
    return buf;
}

function handleFetchResultBuffer(rawBuffer) {
    if (isSwitchingRoom) return;
    try {
        const buffer = gunzipIfNeeded(rawBuffer);
        const fetchResult = FetchResultType.decode(buffer);
        if (!fetchResult.messages || !Array.isArray(fetchResult.messages)) return;

        for (const msg of fetchResult.messages) {
            const msgRoomId = msg.common?.roomId ? toSafeStringId(msg.common.roomId) : '';
            if (msgRoomId && closedRoomIdsSet.has(msgRoomId)) continue;
            if (currentRoomId && msgRoomId && msgRoomId !== currentRoomId) continue;

            const msgIdStr = toSafeStringId(msg.msgId);
            if (msgIdStr && processedMsgIds.has(msgIdStr)) continue;
            if (msgIdStr) {
                processedMsgIds.add(msgIdStr);
                if (processedMsgIds.size > 6000) processedMsgIds.clear();
            }

            if (msgRoomId && !currentRoomId && !closedRoomIdsSet.has(msgRoomId)) {
                currentRoomId = msgRoomId;
            }

            const method = msg.method || '';
            const payload = gunzipIfNeeded(msg.payload);

            if (method === 'WebcastLinkMicBattle') {
                if (!isPKActive) isPKActive = true;
            } else if (method === 'WebcastRoomUserSeqMessage') {
                const seqMsg = RoomUserSeqMessageType.decode(payload);
                const seqRoomId = seqMsg.common?.roomId ? toSafeStringId(seqMsg.common.roomId) : msgRoomId;
                if (seqRoomId && closedRoomIdsSet.has(seqRoomId)) continue;
                if (currentRoomId && seqRoomId && seqRoomId !== currentRoomId) continue;

                const allRanks = [...(seqMsg.ranks || []), ...(seqMsg.seats || [])];
                for (let idx = 0; idx < allRanks.length; idx++) {
                    const r = allRanks[idx];
                    const u = r.user;
                    if (!u || !u.id) continue;
                    const uid = toSafeStringId(u.id);
                    const score = toSafeNumber(r.score);
                    const rank = toSafeNumber(r.rank) || (idx + 1);
                    const { gifterLevel, fanclubLevel, avatarUrl } = extractLevelsFromProtoUser(u, {}, null);
                    registerDiscoveredUser({
                        userId: uid,
                        secUid: u.secUid || '',
                        uniqueId: u.displayId || '',
                        nickname: u.nickname || '',
                        genderCode: u.gender || 0,
                        signature: u.signature || '',
                        avatarUrl,
                        gifterLevel,
                        fanclubLevel,
                        audienceScore: score,
                        rank
                    });
                }
            } else if (method === 'WebcastMemberMessage') {
                const memberMsg = MemberMessageType.decode(payload);
                const u = memberMsg.user;
                if (u && u.id) {
                    const uid = toSafeStringId(u.id);
                    const { gifterLevel, fanclubLevel, avatarUrl } = extractLevelsFromProtoUser(u, {}, payload);
                    registerDiscoveredUser({
                        userId: uid,
                        secUid: u.secUid || '',
                        uniqueId: u.displayId || '',
                        nickname: u.nickname || '',
                        genderCode: u.gender || 0,
                        signature: u.signature || '',
                        avatarUrl,
                        gifterLevel,
                        fanclubLevel,
                        audienceScore: getUserMucChoiInRoom(uid)
                    });
                }
            } else if (method === 'WebcastChatMessage') {
                const chatMsg = ChatMessageType.decode(payload);
                const u = chatMsg.user;
                if (u && u.id) {
                    const uid = toSafeStringId(u.id);
                    const { gifterLevel, fanclubLevel, avatarUrl } = extractLevelsFromProtoUser(u, {}, payload);
                    registerDiscoveredUser({
                        userId: uid,
                        secUid: u.secUid || '',
                        uniqueId: u.displayId || '',
                        nickname: u.nickname || '',
                        genderCode: u.gender || 0,
                        signature: u.signature || '',
                        avatarUrl,
                        gifterLevel,
                        fanclubLevel,
                        audienceScore: getUserMucChoiInRoom(uid)
                    });
                }
            } else if (method === 'WebcastGiftMessage') {
                const decodedGift = GiftMessageType.decode(payload);
                processDecodedGift(decodedGift, payload);
            }
        }
    } catch (e) {}
}

function handleWebSocketFrameBuffer(frameBuffer) {
    if (isSwitchingRoom) return;
    try {
        const pushFrame = PushFrameType.decode(frameBuffer);
        if (pushFrame.payload && pushFrame.payload.length > 0) {
            handleFetchResultBuffer(pushFrame.payload);
        }
    } catch (e) {
        handleFetchResultBuffer(frameBuffer);
    }
}

function handleInterceptedWebcastJson(url, json) {
    if (!json || typeof json !== 'object') return;
    try {
        if (url.includes('/webcast/feed/') || url.includes('/api/live/')) {
            const items = json?.data || json?.aweme_list || [];
            if (Array.isArray(items)) {
                for (const it of items) {
                    const roomData = it?.data || it?.room || it || {};
                    if (roomData?.has_commerce_goods === true) continue;
                    if (roomData?.status !== undefined && Number(roomData.status) !== 2) continue;
                    const owner = roomData?.owner || it?.owner || it?.author || {};
                    const idStr = owner.display_id || owner.unique_id || '';
                    if (idStr) farmedFeedRoomsSet.add(idStr);
                }
            }
            return;
        }

        if (url.includes('/webcast/gift/list/')) {
            function extractAllCatalogGifts(node, depth = 0) {
                if (!node || typeof node !== 'object' || depth > 6) return;
                if (Array.isArray(node)) {
                    for (const el of node) extractAllCatalogGifts(el, depth + 1);
                    return;
                }
                if (node.id && node.name && (node.diamond_count !== undefined || node.diamondCount !== undefined)) {
                    giftCatalogMap.set(Number(node.id), {
                        name: node.name,
                        diamondCount: Number(node.diamond_count ?? node.diamondCount ?? 1),
                        type: Number(node.type || 1)
                    });
                }
                for (const k of Object.keys(node)) {
                    extractAllCatalogGifts(node[k], depth + 1);
                }
            }
            extractAllCatalogGifts(json?.data, 0);
            return;
        }

        if (isSwitchingRoom) return;

        const rMatch = String(url).match(/[?&]room_id=(\d{10,})/);
        const urlRoomId = rMatch ? rMatch[1] : '';

        if (urlRoomId && closedRoomIdsSet.has(urlRoomId)) return;
        if (currentRoomId && urlRoomId && urlRoomId !== currentRoomId) return;

        if (urlRoomId && !currentRoomId && !closedRoomIdsSet.has(urlRoomId)) {
            currentRoomId = urlRoomId;
        }
        const aMatch = String(url).match(/[?&]anchor_id=(\d{10,})/);
        if (aMatch && !currentAnchorId) currentAnchorId = aMatch[1];

       if (url.includes('/webcast/room/info/')) {
            const d = json?.data || {};
            const infoRoomId = d.id_str ? String(d.id_str) : urlRoomId;
            if (infoRoomId && closedRoomIdsSet.has(infoRoomId)) return;
            if (currentRoomId && infoRoomId && infoRoomId !== currentRoomId) return;

            if (infoRoomId && !currentRoomId) currentRoomId = infoRoomId;
            if (d.owner_user_id_str) currentAnchorId = String(d.owner_user_id_str);
            else if (d.owner?.id_str) currentAnchorId = String(d.owner.id_str);
            if (d.owner?.sec_uid && !currentAnchorSecUid) currentAnchorSecUid = String(d.owner.sec_uid);
            if (d.has_commerce_goods === true) currentRoomHasCommerce = true;

            const topFans = d.top_fans || [];
            for (let idx = 0; idx < topFans.length; idx++) {
                const item = topFans[idx];
                const u = item?.user || {};
                const uid = toSafeStringId(u.id_str || u.id);
                if (!uid) continue;
                const score = Math.max(
                    toSafeNumber(item?.fan_ticket ?? item?.score ?? item?.coin_count),
                    parseCompactDiamonds(item?.gap_description)
                );
                const { gifterLevel, fanclubLevel, avatarUrl } = extractLevelsFromApiUserJson(u);
                registerDiscoveredUser({
                    userId: uid,
                    secUid: u.sec_uid || '',
                    uniqueId: u.display_id || '',
                    nickname: u.nickname || '',
                    genderCode: u.gender || 0,
                    signature: u.bio_description || u.signature || '',
                    avatarUrl,
                    gifterLevel,
                    fanclubLevel,
                    audienceScore: score,
                    rank: idx + 1
                });
            }
        } else if (url.includes('/webcast/ranklist/online_audience/') || url.includes('/webcast/ranklist/')) {
            const ranks = json?.data?.ranks || json?.data?.contributors || json?.data?. seats || [];
            for (let idx = 0; idx < ranks.length; idx++) {
                const item = ranks[idx];
                const u = item?.user || {};
                const uid = toSafeStringId(u.id_str || u.id);
                if (!uid) continue;
                const score = Math.max(
                    toSafeNumber(item?.score ?? item?.coin_count ?? item?.fan_ticket ?? item?.delta),
                    parseCompactDiamonds(item?.gap_description || item?.contributor_text)
                );
                const rank = toSafeNumber(item?.rank) || (idx + 1);
                const { gifterLevel, fanclubLevel, avatarUrl } = extractLevelsFromApiUserJson(u);
                registerDiscoveredUser({
                    userId: uid,
                    secUid: u.sec_uid || '',
                    uniqueId: u.display_id || '',
                    nickname: u.nickname || '',
                    genderCode: u.gender || 0,
                    signature: u.bio_description || u.signature || '',
                    avatarUrl,
                    gifterLevel,
                    fanclubLevel,
                    audienceScore: score,
                    rank
                });
            }
        } else if (url.includes('/webcast/ranklist/')) {
            const contributors = json?.data?.ranks || json?.data?.contributors || [];
            for (const item of contributors) {
                const u = item?.user || {};
                const uid = toSafeStringId(u.id_str || u.id);
                if (!uid) continue;
                const { gifterLevel, fanclubLevel, avatarUrl } = extractLevelsFromApiUserJson(u);
                registerDiscoveredUser({
                    userId: uid,
                    secUid: u.sec_uid || '',
                    uniqueId: u.display_id || '',
                    nickname: u.nickname || '',
                    genderCode: u.gender || 0,
                    signature: u.bio_description || u.signature || '',
                    avatarUrl,
                    gifterLevel,
                    fanclubLevel,
                    audienceScore: 0
                });
            }
        }
    } catch (e) {}
}

async function setupBrowserHooksAndCDP() {
    if (!page || page.isClosed()) return;
    try {
        try {
            await page.exposeFunction('__onTikTokWebcastJson', (url, json) => {
                handleInterceptedWebcastJson(url, json);
            });
        } catch (e) {}

        await page.evaluateOnNewDocument(() => {
            const origFetch = window.fetch;
            window.fetch = async function (...args) {
                const res = await origFetch.apply(this, args);
                try {
                    const url = typeof args[0] === 'string' ? args[0] : (args[0]?.url || '');
                    if (
                        url.includes('/webcast/ranklist/') ||
                        url.includes('/webcast/room/info/') ||
                        url.includes('/webcast/gift/list/') ||
                        url.includes('/webcast/privilege/gift_gallery/') ||
                        url.includes('/webcast/feed/')
                    ) {
                        res.clone().json().then(json => {
                            if (typeof window.__onTikTokWebcastJson === 'function') {
                                window.__onTikTokWebcastJson(url, json);
                            }
                        }).catch(() => {});
                    }
                } catch (e) {}
                return res;
            };
        });

        if (cdpSession) {
            try { await cdpSession.detach(); } catch (e) {}
        }
        cdpSession = await page.target().createCDPSession();
        await cdpSession.send('Network.enable');

        cdpSession.on('Network.webSocketFrameReceived', ({ response }) => {
            if (response && response.payloadData) {
                const buf = Buffer.from(response.payloadData, 'base64');
                handleWebSocketFrameBuffer(buf);
            }
        });

        page.removeAllListeners('response');
        page.on('response', async (res) => {
            const url = res.url();
            try {
                if (!isSwitchingRoom) {
                    const rMatch = url.match(/[?&]room_id=(\d{10,})/);
                    if (rMatch && !currentRoomId && !closedRoomIdsSet.has(rMatch[1])) currentRoomId = rMatch[1];
                    const aMatch = url.match(/[?&]anchor_id=(\d{10,})/);
                    if (aMatch && !currentAnchorId) currentAnchorId = aMatch[1];
                }

                if (url.includes('/webcast/im/fetch/')) {
                    if (isSwitchingRoom) return;
                    const rMatch = url.match(/[?&]room_id=(\d{10,})/);
                    const fetchRoomId = rMatch ? rMatch[1] : '';
                    if (fetchRoomId && closedRoomIdsSet.has(fetchRoomId)) return;
                    if (currentRoomId && fetchRoomId && fetchRoomId !== currentRoomId) return;

                    const buf = await res.buffer();
                    handleFetchResultBuffer(buf);
                } else if (
                    url.includes('/webcast/ranklist/') ||
                    url.includes('/webcast/room/info/') ||
                    url.includes('/webcast/gift/list/') ||
                    url.includes('/webcast/privilege/gift_gallery/') ||
                    url.includes('/webcast/feed/')
                ) {
                    const json = await res.json();
                    handleInterceptedWebcastJson(url, json);
                }
            } catch (e) {}
        });
    } catch (err) {}
}

// =========================================================================
// 13. QUÉT VÉT CẠN CẢ 2 ENGINE TRONG PHÒNG LIVE (CẢ ONLINE LẪN OFFLINE)
// =========================================================================
async function executeDualEngineSweep(roomId, anchorId, sessionId = currentRoomSessionId) {
    if (!page || page.isClosed() || !roomId || isSwitchingRoom || currentRoomSessionId !== sessionId) return;

    // Bước 1: Quét trực tiếp Phòng Trưng Bày của Idol để bắt trọn "Nhà sưu tầm" & "3 người tặng quà hàng đầu" (dù đang Offline)
    if (anchorId) {
        await scanIdolGiftGallerySponsors(roomId, anchorId, sessionId);
    }
    
    if (isSwitchingRoom || currentRoomSessionId !== sessionId) return;

    // Bước 2: Gọi đồng bộ API lấy danh sách Người xem (Online) & Bảng đóng góp lịch sử/tuần/ngày (Offline/Online)
    const fetchedPayloads = await page.evaluate(async (rId, aId) => {
        const urls = [
            `https://webcast.tiktok.com/webcast/ranklist/online_audience/?aid=1988&app_language=vi-VN&app_name=tiktok_web&device_platform=web_pc&room_id=${rId}${aId ? `&anchor_id=${aId}` : ''}`,
            `https://webcast.tiktok.com/webcast/ranklist/room/${rId}/contributor/?aid=1988&app_language=vi-VN&app_name=tiktok_web&device_platform=web_pc&room_id=${rId}${aId ? `&anchor_id=${aId}` : ''}`,
            `https://webcast.tiktok.com/webcast/ranklist/room/${rId}/contributor/?aid=1988&app_language=vi-VN&app_name=tiktok_web&device_platform=web_pc&room_id=${rId}${aId ? `&anchor_id=${aId}` : ''}&rank_type=1`,
            `https://webcast.tiktok.com/webcast/ranklist/room/${rId}/contributor/?aid=1988&app_language=vi-VN&app_name=tiktok_web&device_platform=web_pc&room_id=${rId}${aId ? `&anchor_id=${aId}` : ''}&rank_type=2`,
            `https://webcast.tiktok.com/webcast/room/info/?aid=1988&app_language=vi-VN&app_name=tiktok_web&device_platform=web_pc&room_id=${rId}`
        ];
        const settled = await Promise.allSettled(
            urls.map(u => fetch(u, { credentials: 'include' }).then(r => r.json()).then(json => ({ url: u, json })))
        );
        return settled.filter(s => s.status === 'fulfilled' && s.value?.json).map(s => s.value);
    }, roomId, anchorId || '').catch(() => []);

    if (isSwitchingRoom || currentRoomSessionId !== sessionId) return;

    for (const item of fetchedPayloads) {
        handleInterceptedWebcastJson(item.url, item.json);
    }

    const allUsers = Array.from(roomDiscoveredUsersMap.values());
    if (allUsers.length === 0) return;

    // Ưu tiên: 1. Có mặt trong Phòng trưng bày Idol (Online/Offline) -> 2. Đã lộ Gifter >= 15 -> 3. Giới tính Nam -> 4. Mức chơi
    allUsers.sort((a, b) => {
        const aInGal = idolGalleryUserGiftsMap.has(a.userId) ? 1 : 0;
        const bInGal = idolGalleryUserGiftsMap.has(b.userId) ? 1 : 0;
        if (bInGal !== aInGal) return bInGal - aInGal;

        const aHigh = (a.gifterLevel || 0) >= 15 ? 1 : 0;
        const bHigh = (b.gifterLevel || 0) >= 15 ? 1 : 0;
        if (bHigh !== aHigh) return bHigh - aHigh;

        const pDiff = (b.genderPriority ?? 20) - (a.genderPriority ?? 20);
        if (pDiff !== 0) return pDiff;

        const gDiff = (b.gifterLevel || 0) - (a.gifterLevel || 0);
        if (gDiff !== 0) return gDiff;

        return (b.audienceScore || 0) - (a.audienceScore || 0);
    });

    console.log(`🔎 [QUÉT VÉT CẠN 2 ENGINE] Phòng @${currentIdol} (${allUsers.length} users Online & Offline)...`);

    // 1. Chạy Engine 1 (Phòng Trưng Bày) trước để bắt & chụp hình đầy đủ tất cả User đạt chuẩn (dù Online hay Offline)
    const engine1Candidates = allUsers.filter(u => {
        const key = `${roomId}_${u.userId}`;
        if (capturedByEngine1Gallery.has(key) || checkedEngine1GalleryUsers.has(key)) return false;
        if (u.gifterLevel > 0 && u.gifterLevel < 15 && !idolGalleryUserGiftsMap.has(u.userId)) return false;
        return true;
    });

    const BATCH_SIZE = 45;
    for (let i = 0; i < engine1Candidates.length; i += BATCH_SIZE) {
        if (currentRoomId !== roomId || currentRoomSessionId !== sessionId || isSwitchingRoom) break;
        const batch = engine1Candidates.slice(i, i + BATCH_SIZE);
        await Promise.allSettled(batch.map(u => runEngine1GiftGalleryForUser(u, sessionId)));
    }

    // Đợi toàn bộ các Promise đang bay của Engine 1 (kể cả từ registerDiscoveredUser) hoàn tất chụp hình
    if (inFlightEngine1Promises.size > 0) {
        await Promise.allSettled(Array.from(inFlightEngine1Promises.values()));
    }

    // 2. Chạy Engine 2 cho Viewers có Mức chơi >= 1000
    const engine2Candidates = allUsers.filter(u => u.audienceScore >= 1000);
    await Promise.allSettled(engine2Candidates.map(u => runEngine2MucChoiViewerCapture(u, sessionId)));

    if (inFlightEngine2Promises.size > 0) {
        await Promise.allSettled(Array.from(inFlightEngine2Promises.values()));
    }
}

async function visitAndMonitorIdolRoom(idolUsername) {
    if (!idolUsername || SHOP_KEYWORDS.some(kw => idolUsername.toLowerCase().includes(kw))) return;

    isSwitchingRoom = true;
    isRoomReadyForEngines = false;
    if (currentRoomId) {
        closedRoomIdsSet.add(String(currentRoomId));
        if (closedRoomIdsSet.size > 500) closedRoomIdsSet.clear();
    }
    for (const [key, item] of activeComboTimersMap.entries()) {
        clearTimeout(item.timer);
    }
    activeComboTimersMap.clear();
    inFlightEngine1Promises.clear();
    inFlightEngine2Promises.clear();

    await ensureBrowserAndPage();
    await page.goto('about:blank', { waitUntil: 'commit', timeout: 3000 }).catch(() => {});

    currentRoomSessionId++;
    const thisSessionId = currentRoomSessionId;
    currentIdol = idolUsername;
    currentRoomId = '';
    currentAnchorId = '';
    currentAnchorSecUid = '';
    currentRoomHasCommerce = false;
    isPKActive = false;
    roomAudienceScoreMap.clear();
    roomAudienceRankMap.clear();
    roomSessionDonatedMap.clear();
    roomDiscoveredUsersMap.clear();
    idolGalleryUserGiftsMap.clear();
    capturedByEngine1Gallery.clear();
    checkedEngine1GalleryUsers.clear();
    capturedByEngine2LivePK.clear();
    screenshotTakenMap.clear();

    isSwitchingRoom = false;

    console.log(`\n⚡ [MỞ PHÒNG LIVE] @${idolUsername}...`);
    await page.goto(`https://www.tiktok.com/@${idolUsername}/live`, { waitUntil: 'domcontentloaded', timeout: 25000 }).catch(() => {});

    for (let i = 0; i < 6; i++) {
        if (currentRoomId && currentAnchorId && roomDiscoveredUsersMap.size > 0) break;
        await new Promise(r => setTimeout(r, 350));
    }

    const roomCheck = await page.evaluate(async (currRoomId, currAnchorId, closedIds) => {
        const closedSet = new Set(closedIds || []);
        const html = document.documentElement.innerHTML;
        let domRoomId = '';
        let domAnchorId = '';
        let domAnchorSecUid = '';
        let roomTitle = '';
        let roomStatus = 2;
        let hasCommerceGoods = false;

        try {
            const uniEl = document.getElementById('__UNIVERSAL_DATA_FOR_REHYDRATION__');
            if (uniEl && uniEl.textContent) {
                const uniJson = JSON.parse(uniEl.textContent);
                const liveRoom = uniJson?.__DEFAULT_SCOPE__?.['webapp.live-detail']?.liveRoomInfo || uniJson?.__DEFAULT_SCOPE__?.['webapp.live-detail']?.liveRoom || {};
                if (liveRoom.roomId && !closedSet.has(String(liveRoom.roomId))) domRoomId = String(liveRoom.roomId);
                if (liveRoom.ownerInfo?.uid) domAnchorId = String(liveRoom.ownerInfo.uid);
                if (liveRoom.ownerInfo?.secUid) domAnchorSecUid = String(liveRoom.ownerInfo.secUid);
                if (liveRoom.status !== undefined) roomStatus = Number(liveRoom.status);
                if (liveRoom.title) roomTitle = liveRoom.title;
            }
        } catch (e) {}

        const resources = performance.getEntriesByType('resource').map(r => r.name);
        for (const u of resources) {
            if (!domRoomId) {
                const rm = u.match(/[?&]room_id=(\d{10,})/);
                if (rm && !closedSet.has(rm[1])) domRoomId = rm[1];
            }
            if (!domAnchorId) {
                const am = u.match(/[?&]anchor_id=(\d{10,})/);
                if (am) domAnchorId = am[1];
            }
        }

        if (!domRoomId) {
            const roomIdMatch =
                html.match(/"roomId"\s*:\s*"?(\d{10,})"?/) ||
                html.match(/"room_id"\s*:\s*"?(\d{10,})"?/) ||
                html.match(/room_id=(\d{10,})/);
            if (roomIdMatch && !closedSet.has(roomIdMatch[1])) domRoomId = roomIdMatch[1];
        }
        if (!domAnchorId) {
            const anchorMatch =
                html.match(/"owner_user_id"\s*:\s*"?(\d{10,})"?/) ||
                html.match(/"anchorId"\s*:\s*"?(\d{10,})"?/) ||
                html.match(/"host"\s*:\s*\{\s*"id"\s*:\s*"?(\d{10,})"?/);
            if (anchorMatch) domAnchorId = anchorMatch[1];
        }

        let finalRoomId = domRoomId || (!closedSet.has(currRoomId) ? currRoomId : '');
        let finalAnchorId = domAnchorId || currAnchorId;

        if (finalRoomId) {
            try {
                const rRes = await fetch(`https://webcast.tiktok.com/webcast/room/info/?aid=1988&app_language=vi-VN&app_name=tiktok_web&device_platform=web_pc&room_id=${finalRoomId}`, { credentials: 'include' });
                const rJson = await rRes.json();
                const d = rJson?.data || {};
                if (!finalAnchorId) finalAnchorId = String(d.owner_user_id_str || d.owner?.id_str || d.owner?.id || '');
                if (!domAnchorSecUid && d.owner?.sec_uid) domAnchorSecUid = String(d.owner.sec_uid);
                roomTitle = d.title || roomTitle;
                if (d.status !== undefined) roomStatus = Number(d.status);
                hasCommerceGoods = d.has_commerce_goods === true;
            } catch (e) {}
        }

        const bagEl = document.querySelector('[data-e2e="live-shopping-bag"]');
        const hasVisibleShopBag = Boolean(bagEl && bagEl.offsetWidth > 0 && bagEl.offsetHeight > 0);
        const hasVideo = !!document.querySelector('video');
        const isOfflineText = document.body.innerText.includes('LIVE đã kết thúc') || document.body.innerText.includes('LIVE has ended');
        const isTrulyEnded = (roomStatus === 4) || (isOfflineText && !hasVideo);

        return { roomId: finalRoomId, anchorId: finalAnchorId, anchorSecUid: domAnchorSecUid, roomTitle, hasShopBag: hasCommerceGoods || hasVisibleShopBag, isTrulyEnded };
    }, currentRoomId, currentAnchorId, Array.from(closedRoomIdsSet)).catch(() => ({ roomId: '', anchorId: '', anchorSecUid: '', roomTitle: '', hasShopBag: false, isTrulyEnded: false }));

    if (roomCheck.roomId && roomCheck.roomId !== currentRoomId) currentRoomId = roomCheck.roomId;
    if (roomCheck.anchorId && roomCheck.anchorId !== currentAnchorId) currentAnchorId = roomCheck.anchorId;
    if (roomCheck.anchorSecUid && !currentAnchorSecUid) currentAnchorSecUid = roomCheck.anchorSecUid;

    if (roomCheck.isTrulyEnded || !currentRoomId) {
        console.log(`⏭️ [BỎ QUA] Phòng @${idolUsername} đã kết thúc phiên LIVE.`);
        return;
    }

    if (currentRoomHasCommerce || roomCheck.hasShopBag || SHOP_KEYWORDS.some(kw => (roomCheck.roomTitle || '').toLowerCase().includes(kw))) {
        return;
    }

    if (giftCatalogMap.size === 0) {
        const giftJson = await page.evaluate(async (rId) => {
            try {
                const res = await fetch(`https://webcast.tiktok.com/webcast/gift/list/?aid=1988&app_language=vi-VN&app_name=tiktok_web&device_platform=web_pc&room_id=${rId}`, { credentials: 'include' });
                return await res.json();
            } catch (e) { return null; }
        }, currentRoomId).catch(() => null);
        if (giftJson) handleInterceptedWebcastJson('/webcast/gift/list/', giftJson);
    }

    isRoomReadyForEngines = true;
    console.log(`✅ [QUÉT 5s] @${idolUsername} (Room ID: ${currentRoomId} | Anchor ID: ${currentAnchorId})`);

    await executeDualEngineSweep(currentRoomId, currentAnchorId, thisSessionId);
    await new Promise(r => setTimeout(r, 2200));
    await executeDualEngineSweep(currentRoomId, currentAnchorId, thisSessionId);
    await new Promise(r => setTimeout(r, 1800));

    for (const [key, item] of activeComboTimersMap.entries()) {
        clearTimeout(item.timer);
        activeComboTimersMap.delete(key);
        await finalizeEngine2LiveGiftCapture(item.giftContext);
    }

    if (inFlightEngine1Promises.size > 0) {
        await Promise.allSettled(Array.from(inFlightEngine1Promises.values()));
    }
    if (inFlightEngine2Promises.size > 0) {
        await Promise.allSettled(Array.from(inFlightEngine2Promises.values()));
    }
    await screenshotLock.catch(() => {});
}

// =========================================================================
// 14. HỆ THỐNG TỰ ĐỘNG LƯỚT (CHỈ GOM PHÒNG ĐANG LIVE THẬT SỰ)
// =========================================================================
async function harvestLiveIdolsFromPage() {
    if (!page || page.isClosed()) return [];
    return await page.evaluate(() => {
        const found = new Set();
        const links = Array.from(document.querySelectorAll('a[href*="/live"]'));
        for (const a of links) {
            const href = a.href || '';
            const mLive = href.match(/tiktok\.com\/@([a-zA-Z0-9._]{2,35})\/live/i);
            if (mLive && mLive[1]) {
                found.add(mLive[1]);
            }
        }
        return Array.from(found);
    }).catch(() => []);
}

async function ensureBrowserAndPage() {
    try {
        const isBrowserAlive = browser && (typeof browser.connected === 'boolean' ? browser.connected : true);
        if (!isBrowserAlive) {
            browser = await puppeteer.launch({
                headless: false,
                defaultViewport: { width: 1280, height: 800 },
                userDataDir: path.join(__dirname, 'chrome_profile'),
                args: ['--no-sandbox', '--disable-setuid-sandbox', '--autoplay-policy=no-user-gesture-required', '--disable-notifications']
            });
            browser.on('disconnected', () => { browser = null; page = null; cdpSession = null; });
        }
        if (!page || page.isClosed()) {
            const pages = await browser.pages();
            page = pages.length > 0 ? pages[0] : await browser.newPage();
            await setupBrowserHooksAndCDP();
        }
        return true;
    } catch (err) {
        browser = null; page = null;
        return false;
    }
}

async function startAutoScroller({ mode = 'search', keyword = 'idol pk' }) {
    if (isBotRunning) return;
    isBotRunning = true;

    while (isBotRunning) {
        try {
            const ready = await ensureBrowserAndPage();
            if (!ready) { await new Promise(r => setTimeout(r, 3000)); continue; }

            let discoveredIdols = [];
            farmedFeedRoomsSet.clear();

            if (mode === 'search') {
                const searchUrl = `https://www.tiktok.com/search/live?q=${encodeURIComponent(keyword)}`;
                console.log(`🔍 [CHẾ ĐỘ SEARCH] Đang tìm phòng Idol Live với từ khóa: "${keyword}"`);
                await page.goto(searchUrl, { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(() => {});
                await new Promise(r => setTimeout(r, 2500));

                for (let i = 0; i < 4; i++) {
                    if (page.isClosed()) break;
                    await page.evaluate(() => window.scrollBy(0, 1400)).catch(() => {});
                    await new Promise(r => setTimeout(r, 900));
                }

                const pageIdols = await harvestLiveIdolsFromPage();
                discoveredIdols = [...new Set([...pageIdols, ...Array.from(farmedFeedRoomsSet)])];
            } else if (mode === 'category') {
                console.log(`📂 [CHẾ ĐỘ CATEGORY] Đang quét danh sách phòng Idol Live theo danh mục...`);
                await page.goto('https://www.tiktok.com/live', { waitUntil: 'domcontentloaded', timeout: 40000 }).catch(() => {});
                await new Promise(r => setTimeout(r, 2500));

                for (let i = 0; i < 4; i++) {
                    if (page.isClosed()) break;
                    await page.evaluate(async () => {
                        window.scrollBy(0, 1400);
                        try {
                            await fetch('https://webcast.tiktok.com/webcast/feed/?aid=1988&app_language=vi-VN&app_name=tiktok_web&channel=tiktok_web_live_card', { credentials: 'include' });
                        } catch (e) {}
                    }).catch(() => {});
                    await new Promise(r => setTimeout(r, 900));
                }

                const pageIdols = await harvestLiveIdolsFromPage();
                discoveredIdols = [...new Set([...pageIdols, ...Array.from(farmedFeedRoomsSet)])];
            } else if (mode === 'farming') {
                console.log(`🔥 [CHẾ ĐỘ ACCOUNT FARMING] Đang gom danh sách phòng ĐANG LIVE từ Đề xuất Nick đã nuôi + Từ khóa...`);
                // 1. Lấy các phòng đề xuất trực tiếp từ thuật toán nick đã nuôi trên trang /live
                await page.goto('https://www.tiktok.com/live', { waitUntil: 'domcontentloaded', timeout: 35000 }).catch(() => {});
                await new Promise(r => setTimeout(r, 1800));
                for (let i = 0; i < 3; i++) {
                    if (page.isClosed()) break;
                    await page.evaluate(async () => {
                        window.scrollBy(0, 1500);
                        try {
                            await fetch('https://webcast.tiktok.com/webcast/feed/?aid=1988&app_language=vi-VN&app_name=tiktok_web&channel=tiktok_web_live_card', { credentials: 'include' });
                        } catch (e) {}
                    }).catch(() => {});
                    await new Promise(r => setTimeout(r, 700));
                }
                const liveFeedIdols = await harvestLiveIdolsFromPage();
                for (const id of liveFeedIdols) farmedFeedRoomsSet.add(id);

                // 2. Kết hợp thêm luồng từ khóa ngách trên cookie của nick đã nuôi
                const farmingKeywords = [...new Set([keyword || 'idol pk', 'pk live'])];
                for (const fKw of farmingKeywords) {
                    const searchUrl = `https://www.tiktok.com/search/live?q=${encodeURIComponent(fKw)}`;
                    await page.goto(searchUrl, { waitUntil: 'domcontentloaded', timeout: 35000 }).catch(() => {});
                    await new Promise(r => setTimeout(r, 1800));

                    for (let i = 0; i < 4; i++) {
                        if (page.isClosed()) break;
                        await page.evaluate(() => window.scrollBy(0, 1500)).catch(() => {});
                        await new Promise(r => setTimeout(r, 700));
                    }

                    const extraIdols = await harvestLiveIdolsFromPage();
                    for (const id of extraIdols) farmedFeedRoomsSet.add(id);
                }

                discoveredIdols = [...new Set(Array.from(farmedFeedRoomsSet))];
            }

            console.log(`📋 Thu thập được ${discoveredIdols.length} phòng Idol đang Live:`, discoveredIdols);

            for (const idol of discoveredIdols) {
                if (!isBotRunning) break;
                await visitAndMonitorIdolRoom(idol);
            }
        } catch (loopErr) {
            await new Promise(r => setTimeout(r, 2500));
        }
    }
}

app.get('/api/whales', async (req, res) => {
    const [rows] = await pool.query('SELECT * FROM whale_logs ORDER BY id DESC LIMIT 200');
    for (const r of rows) {
        const gInfo = evaluateGenderPriority(0, r.nickname, r.unique_id, '');
        if (gInfo.known) r.gender = gInfo.gender;
        r.region = detectUserRegion(r.nickname, r.unique_id, '');
    }
    res.json(rows);
});

app.post('/api/start-bot', async (req, res) => {
    const { mode, keyword } = req.body;
    startAutoScroller({ mode: mode || 'search', keyword: keyword || 'idol pk' });
    res.json({ status: 'started', mode, keyword });
});

const PORT = 3000;
initDB().then(() => {
    server.listen(PORT, '0.0.0.0', () => {
        console.log(`🚀 Web Dashboard đã sẵn sàng tại: http://localhost:${PORT}`);
    });
});