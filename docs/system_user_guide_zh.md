# A鑲″崐鑷姩浜ゆ槗杈呭姪绯荤粺浣跨敤鎵嬪唽锛圵indows锛?
鏇存柊鏃堕棿锛?026-02-23
閫傜敤鐗堟湰锛氬綋鍓嶄粨搴撲富骞诧紙鍚嚜鍔ㄨ皟鍙傘€佹寫鎴樿禌銆佹寔浠撳垎鏋愩€佸噯纭€т笌涓婄嚎鍑嗗叆鐪嬫澘锛?
## 1. 绯荤粺瀹氫綅涓庤竟鐣?
鏈郴缁熸槸鈥滄姇鐮斾笌浜ゆ槗鍐崇瓥鏀寔骞冲彴鈥濓紝涓嶆槸鑷姩涓嬪崟绯荤粺銆?
- 鏀寔锛氭暟鎹媺鍙栥€佸洜瀛愯绠椼€佺瓥鐣ヤ俊鍙枫€侀鎺с€佸洖娴嬨€佺粍鍚堜紭鍖栥€佹墽琛屽洖鍐欍€佸鐩樸€佸璁°€佽繍缁寸洃鎺с€?- 涓嶆敮鎸侊細鍒稿晢 API 鑷姩涓嬪崟銆佹敹鐩婁繚璇併€侀浂鍥炴挙淇濊瘉銆?- 缁撹锛氱郴缁熺洰鏍囨槸鈥滄彁鍗囪儨鐜囦笌鎵ц璐ㄩ噺銆佹帶鍒堕闄┾€濓紝涓嶆槸鈥滀繚璇佷笉浜忛挶鈥濄€?
## 2. 椤甸潰涓庡鑸€昏

绯荤粺鍓嶇鍏ュ彛鍏?3 涓細

1. 涓荤晫闈細`/ui/`
2. 鎶曠爺浜ゆ槗宸ヤ綔鍙帮細`/trading/workbench`
3. 杩愮淮鐪嬫澘锛歚/ops/dashboard`

宸ヤ綔鍙板唴鏈?6 涓富鏍囩椤碉細

1. 绛栫暐涓庡弬鏁伴〉
2. 鑷姩璋冨弬椤?3. 璺ㄧ瓥鐣ユ寫鎴樿禌椤?4. 缁撴灉鍙鍖栭〉
5. 鎸佷粨鍒嗘瀽椤?6. 浜ゆ槗鍑嗗鍗曚笌鎵ц鍥炲啓椤?
## 3. 鍔熻兘瑕嗙洊鐭╅樀锛堝凡瀹炵幇锛?
| 鍔熻兘鍩?| 宸插疄鐜拌兘鍔?| 涓昏椤甸潰/API |
|---|---|---|
| 鑷姩璋冨弬闃茶繃鎷熷悎 | walk-forward 澶氱獥鍙ｃ€佺ǔ瀹氭€ф儵缃氥€佸弬鏁版紓绉绘儵缃氥€佹敹鐩婃柟宸儵缃?| `POST /autotune/run`銆佽嚜鍔ㄨ皟鍙傞〉 |
| 鑷姩璋冨弬鍓嶇鍖?| 浠诲姟杩愯銆佸€欓€夋銆佸熀绾垮姣斻€佺敾鍍忔縺娲?鍥炴粴銆佺伆搴﹁鍒?| 鑷姩璋冨弬椤点€乣/autotune/*` |
| 鐢诲儚鍥炴粴涓庣伆搴?| 涓€閿洖婊氫笂涓敾鍍忋€佹寜绛栫暐/鎸夋爣鐨勭伆搴﹀惎鐢?| `POST /autotune/profiles/rollback`銆乣/autotune/rollout/rules/*` |
| 璺ㄧ瓥鐣ユ寫鎴樿禌 | 鍏瓥鐣ュ悓绐楄瘎娴嬨€佺‖闂ㄦ绛涢€夈€佸啝鍐涗簹鍐涖€佺伆搴﹁鍒?| 鎸戞垬璧涢〉銆乣POST /challenge/run` |
| 缁勫悎绾у洖娴?| 澶氭爣鐨勫噣鍊笺€佽皟浠撳懆鏈熴€佷粨浣嶄笂闄愩€佽涓?涓婚绾︽潫銆佽祫閲戝埄鐢ㄧ巼 | `POST /backtest/portfolio-run` |
| 鎷熺湡鎴愭湰涓庢垚浜?| 鏈€浣庝剑閲戙€佸嵃鑺辩◣銆佽繃鎴疯垂銆佸啿鍑绘垚鏈€佸垎妗ｆ粦鐐广€佹垚浜ゆ鐜?| 鍥炴祴寮曟搸銆乣/replay/cost-model/*` |
| 椋庢帶澧炲己 | 杩炵画浜忔崯銆佸崟鏃ヤ簭鎹熴€乂aR/ES銆侀泦涓害闃堝€笺€乀+1/ST/鍋滅墝绛?| `POST /risk/check`銆乣POST /portfolio/risk/check` |
| 鐮旂┒-鎵ц闂幆 | 寤鸿鍗?-> 鎵嬪伐鎵ц鍥炲啓 -> 鍋忓樊褰掑洜 -> 鍙傛暟寤鸿 | 鎵ц鍥炲啓椤点€乣/replay/*`銆乣/reports/generate` |
| 鏁版嵁灞傚寮?| 澶氭簮鍥為€€銆佹椂搴忕紦瀛樹笌澧為噺琛ユ媺銆佸瓧娈佃川閲忚瘎鍒?| `data/*`銆乣POST /data/quality/report` |
| 鏁版嵁璁稿彲璇佹不鐞?| 鏁版嵁闆嗘巿鏉冪櫥璁般€佺敤閫旀牎楠屻€佸鍑烘潈闄愭鏌?| `POST /data/licenses/register`銆乣POST /data/licenses/check` |
| 鍥犲瓙蹇収涓庤惤搴撶暀鐥?| 鍥犲瓙瀹炴椂蹇収銆佹暟鎹揩鐓у搱甯岀櫥璁般€佸彲杩芥函 | `GET /factors/snapshot`銆乣POST /data/snapshots/register` |
| 绛栫暐娌荤悊涓庡鎵?| 鐗堟湰娉ㄥ唽銆侀€佸銆佸瑙掕壊瀹℃壒銆佽繍琛屾椂寮哄埗鍙敤宸插鎵圭増鏈?| `/strategy-governance/*` |
| 鐮旂┒娴佹按绾跨紪鎺?| 姣忔棩鐮旂┒绠＄嚎涓€閿繍琛岋紙淇″彿+浜嬩欢澧炲己+灏忚祫閲戣繃婊わ級 | `POST /pipeline/daily-run` |
| 妯″瀷椋庨櫓鐩戞帶 | 绛栫暐婕傜Щ妫€娴嬨€侀闄╁憡璀︺€佸璁＄暀鐥?| `POST /model-risk/drift-check` |
| 绯荤粺閰嶇疆涓庢潈闄?| 鐜閰嶇疆鏌ョ湅銆侀壌鏉冭鑹茶瘑鍒€佹潈闄愮煩闃垫煡璇?| `/system/config`銆乣/system/auth/*` |
| 鐢熶骇杩愮淮涓庡璁?| 浣滀笟璋冨害銆丼LA銆佸憡璀﹂檷鍣€佽瘉鎹寘銆佸璁″搱甯岄摼 | `/ops/*`銆乣/alerts/*`銆乣/compliance/evidence/*` |
| 鎸佷粨鍒嗘瀽闂幆 | 鎵嬪伐鎴愪氦銆佹寔浠撳揩鐓с€佹鏃ラ娴嬨€佸姩浣滃缓璁紙ADD/REDUCE/EXIT/HOLD/BUY_NEW锛?| 鎸佷粨鍒嗘瀽椤点€乣/holdings/*` |
| 绛栫暐鍑嗙‘鎬х湅鏉?| OOS 鍛戒腑鐜囥€丅rier銆佹敹鐩婂亸宸€佹垚鏈悗鏀剁泭銆佹墽琛岃鐩栫巼 | `GET /reports/strategy-accuracy` |
| 涓婄嚎鍑嗗叆涓庡洖婊氬缓璁?| 闂ㄦ鍒ゅ畾銆佽嚜鍔ㄥ洖婊氳Е鍙戝櫒銆佹瘡鏃ラ獙鏀舵竻鍗?| `GET /reports/go-live-readiness` |

## 4. 鍚姩鏂瑰紡涓庣鍙?
### 4.1 涓€閿惎鍔紙鎺ㄨ崘锛?
鍙屽嚮锛歚start_system_windows.bat`

榛樿鍚庣绔彛锛歚127.0.0.1:8000`

鑴氭湰浼氳嚜鍔細

1. 鍒涘缓/澶嶇敤 `.venv`
2. 鍒涘缓/澶嶇敤 `.env`
3. 瀹夎渚濊禆
4. 鍚姩 API
5. 鎵撳紑涓変釜椤甸潰锛歚/ui/`銆乣/trading/workbench`銆乣/ops/dashboard`

### 4.2 鎵嬪姩鍚姩锛圥owerShell锛?
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\run_api.ps1
```

### 4.3 甯歌鍚姩鏃ュ織璇存槑

- `GET /favicon.ico 404`锛氭甯革紝涓嶅奖鍝嶄笟鍔″姛鑳姐€?- `ops_scheduler_enabled bool parsing error`锛歚.env` 鍊煎悗闈㈡湁绌烘牸锛堜緥濡?`true `锛夛紝鏀规垚涓ユ牸 `true`/`false`銆?
## 5. 宸ヤ綔鍙颁娇鐢ㄨ鏄庯紙鎸夋爣绛鹃〉锛?
### 5.1 绛栫暐涓庡弬鏁伴〉

涓昏鐢ㄤ簬鐢熸垚淇″彿銆佸洖娴嬨€佺爺绌朵笌缁勫悎璋冧粨銆?
鍏抽敭鑳藉姏锛?
1. 閫夋嫨绛栫暐涓庡弬鏁帮紙6 绛栫暐锛夈€?2. 閰嶇疆浜嬩欢澧炲己銆佽储鎶ュ寮恒€佸皬璧勯噾妯″紡銆?3. 杩愯锛?   - `杩愯淇″彿鐢熸垚` -> `POST /signals/generate`
   - `杩愯鍥炴祴` -> `POST /backtest/run`
   - `杩愯缁勫悎鍑€鍊煎洖娴媊 -> `POST /backtest/portfolio-run`
   - `杩愯鐮旂┒宸ヤ綔娴乣 -> `POST /research/run`
4. 灏忚祫閲戞ā鏉匡細`2000/5000/8000` 涓€閿鍙傘€?
### 5.2 鑷姩璋冨弬椤?
涓昏鐢ㄤ簬鍙傛暟鎼滅储銆佽繃鎷熷悎鎶戝埗鍜岀敾鍍忕鐞嗐€?
鍏抽敭鑳藉姏锛?
1. 鎼滅储绌洪棿 + 鍩虹嚎鍙傛暟璁剧疆銆?2. walk-forward 澶氱獥鍙ｉ獙璇併€?3. 鍊欓€夋帓琛屾锛坥bjective銆乷verfit銆乻tability銆乸aram drift銆乺eturn variance锛夈€?4. 鍙傛暟鐢诲儚绠＄悊锛?   - 婵€娲荤敾鍍?   - 涓€閿洖婊氫笂涓€涓敾鍍?   - 鏌ョ湅褰撳墠鐢熸晥鐢诲儚
5. 鐏板害瑙勫垯锛氭寜绛栫暐/鎸夋爣鐨勫惎鍋溿€?
### 5.3 璺ㄧ瓥鐣ユ寫鎴樿禌椤?
鐢ㄤ簬鈥滃悓绐楀彛銆佸悓绾︽潫鈥濅笅姣旇緝鍏瓥鐣ュ競鍦洪€傞厤鎬с€?
鍏抽敭鑳藉姏锛?
1. 鍚屾椂璺戝叚绛栫暐锛堟瘡涓瓥鐣ュ厛璋冨弬鍐嶈瘎娴嬶級銆?2. 纭棬妲涚瓫閫夛紙鍥炴挙銆佸鏅€佷氦鏄撴暟銆亀alk-forward 绋冲畾鎬э級銆?3. 缁煎悎璇勫垎鎺掑簭锛堟敹鐩?+ 绋冲畾鎬?- 鍥炴挙 - 鏂瑰樊鎯╃綒锛夈€?4. 杈撳嚭鍐犲啗/浜氬啗涓庣伆搴︿笂绾胯鍒掋€?5. 鍐犲啗鍙傛暟鍙竴閿洖濉埌绛栫暐椤点€?
### 5.4 缁撴灉鍙鍖栭〉

鐢ㄤ簬鐪嬪浘鐪嬭〃骞惰仈鍔ㄨ皟浠撱€?
鍏抽敭鑳藉姏锛?
1. KPI锛氫俊鍙锋暟銆侀樆鏂暟銆佸洖娴嬫敹鐩?鍥炴挙/澶忔櫘绛夈€?2. 淇″彿琛?+ 椋庢帶鏄庣粏鑱斿姩銆?3. K 绾垮彔鍔犱俊鍙风偣浣嶃€?4. 缁勫悎鏉冮噸鍥俱€佽涓氭毚闇插浘銆?5. 璋冧粨璁″垝鑱斿姩锛歚POST /portfolio/rebalance/plan`銆?6. 缁勫悎绾у洖娴嬬粨鏋滃睍绀恒€?
### 5.5 鎸佷粨鍒嗘瀽椤?
鐢ㄤ簬鎵嬪伐浜ゆ槗鍙拌处銆佹寔浠撶敾鍍忋€佹鏃ュ缓璁拰鍑嗙‘鎬у鐩樸€?
#### 5.5.1 鎵嬪伐鎴愪氦褰曞叆锛坄POST /holdings/trades`锛?
瀛楁璇存槑锛?
1. `trade_date`锛氭垚浜ゆ棩鏈?2. `symbol` / `symbol_name`
3. `side`锛欱UY/SELL
4. `price`锛氭垚浜や环
5. `lots` / `lot_size`
6. `fee`锛氭€昏垂鐢?7. `reference_price`锛氬缓璁?鍙傝€冧环锛堢敤浜庢粦鐐硅瘎浼帮級
8. `executed_at`锛氭垚浜ゆ椂闂?9. `is_partial_fill`锛氭槸鍚﹂儴鍒嗘垚浜?10. `unfilled_reason`锛氭湭鎴愪氦鎴栭儴鍒嗘垚浜ゅ師鍥?11. `note`

#### 5.5.2 鎸佷粨蹇収涓庢鏃ュ缓璁?
- 鎸佷粨蹇収锛歚GET /holdings/positions`
- 娆℃棩鍒嗘瀽锛歚POST /holdings/analyze`

寤鸿鍔ㄤ綔鍖呮嫭锛?
1. `ADD`
2. `REDUCE`
3. `EXIT`
4. `HOLD`
5. `BUY_NEW`

姣忔潯寤鸿鍚細`target_lots`銆乣delta_lots`銆乣confidence`銆乣risk_flags`銆佹墽琛屾椂娈靛缓璁瓑銆?
#### 5.5.3 绛栫暐鍑嗙‘鎬х湅鏉?
鎺ュ彛锛歚GET /reports/strategy-accuracy`

鏍稿績鎸囨爣锛?
1. 鏍锋湰澶栧懡涓巼锛坔it rate锛?2. Brier 鍒嗘暟锛堟鐜囨牎鍑嗚宸級
3. 鏀剁泭鍋忓樊锛堥娴?瀹為檯锛?4. 鎴愭湰鍚庡姩浣滄敹鐩?5. 鎵ц瑕嗙洊鐜囷紙寤鸿鏄惁琚洖鍐欐墽琛岋級

#### 5.5.4 涓婄嚎鍑嗗叆闂ㄦ琛?
鎺ュ彛锛歚GET /reports/go-live-readiness`

杈撳嚭鍐呭锛?
1. 闂ㄦ妫€鏌ワ紙pass/fail锛?2. Readiness 绛夌骇锛坄BLOCKED` / `GRAY_READY_WITH_WARNINGS` / `GRAY_READY`锛?3. 鑷姩鍥炴粴瑙﹀彂瑙勫垯
4. 姣忔棩楠屾敹娓呭崟

### 5.6 浜ゆ槗鍑嗗鍗曚笌鎵ц鍥炲啓椤?
鍏抽敭鑳藉姏锛?
1. 鏌ョ湅寤鸿鍗曞苟涓€閿～鍏ユ墽琛屽崟銆?2. 鎵ц鍥炲啓锛歚POST /replay/executions/record`銆?3. 鎵ц澶嶇洏锛歚GET /replay/report`銆?4. 鍋忓樊褰掑洜锛歚GET /replay/attribution`銆?5. closure 鎶ュ憡锛歚POST /reports/generate` (`report_type=closure`)銆?6. 鎴愭湰妯″瀷閲嶄及锛歚POST /replay/cost-model/calibrate`銆?
## 6. 浠庣爺绌跺埌鎵ц鐨勯棴鐜紙鏍囧噯娴佺▼锛?
1. 鍦ㄧ瓥鐣ラ〉鐢熸垚淇″彿銆?2. 鍦ㄧ粨鏋滈〉妫€鏌ラ鎺с€佸洖娴嬨€佺粍鍚堝缓璁€?3. 鎵嬪伐涓嬪崟鍚庡綍鍏ユ寔浠撲氦鏄撳彴璐︺€?4. 鍦ㄦ墽琛岄〉鍥炲啓鎴愪氦銆?5. 鏌ョ湅澶嶇洏涓庡綊鍥狅紝璇嗗埆鍋忓樊鏉ユ簮銆?6. 鍒锋柊鍑嗙‘鎬х湅鏉夸笌涓婄嚎鍑嗗叆鎶ュ憡銆?7. 鑻ヤ笉杈炬爣锛屽洖婊氱敾鍍忓苟璋冩暣鍙傛暟锛屽啀杩涘叆涓嬩竴杞€?
## 7. 鏁版嵁涓庣畻娉曡鏄?
### 7.1 鏁版嵁婧?
1. `tushare` 浼樺厛
2. `akshare` 鍥為€€
3. 鏈湴缂撳瓨锛氭椂搴忓閲忚ˉ鎷夛紙鍑忓皯閲嶅璇锋眰锛?
### 7.2 棰戠巼鏀寔

1. 鏃ョ嚎锛氫富绛栫暐涓庡洖娴嬩富棰?2. 鍒嗛挓绾匡紙1m/5m/15m/30m/60m锛夛細鐢ㄤ簬鎸佷粨椤垫墽琛屾椂娈靛缓璁笌鐩樹腑椋庨櫓杈呭姪

### 7.3 绠楁硶鍘熷垯

1. 涓嶉€夆€滃崟娈靛埄娑︽渶楂樷€濓紝閫夆€滄牱鏈鏇寸ǔ鈥濄€?2. 澶氭寚鏍囩患鍚堣瘎鍒嗚€岄潪鍗曚竴鏀剁泭銆?3. 鍏堣繃纭棬妲涳紝鍐嶅仛鎺掑悕銆?
### 7.4 鍥犲瓙蹇収涓庢ā鍨嬫紓绉?
1. 鍙敤 `GET /factors/snapshot` 鏌ョ湅鎸囧畾鏍囩殑鍦ㄦ寚瀹氬尯闂寸殑鏈€鏂板洜瀛愬€硷紙鍚熀鏈潰澧炲己鍥犲瓙锛夈€?2. 姣忔鍥犲瓙蹇収浼氱櫥璁版暟鎹揩鐓у搱甯岋紝渚夸簬鍥炴祴涓庡疄鐩樺璐﹁拷婧€?3. 鍙敤 `POST /model-risk/drift-check` 鍋氱瓥鐣ユ紓绉绘娴嬶紝闃叉妯″瀷鍦ㄥ競鍦哄垏鎹㈠悗澶辨晥銆?
## 8. 椋庢帶銆佹垚鏈笌鍙氦鏄撴€?
绯荤粺宸茶鐩栵細

1. T+1銆丼T銆佸仠鐗屻€佹定璺屽仠绛夊熀纭€绾︽潫
2. 鍗曠エ浠撲綅涓婇檺銆佽涓?涓婚闆嗕腑搴︾害鏉?3. 杩炵画浜忔崯銆佸崟鏃ヤ簭鎹熴€乂aR/ES 绛夌粍鍚堥闄╂帶鍒?4. 鏈€浣庝剑閲戙€佸嵃鑺辩◣銆佽繃鎴疯垂
5. 鎷熺湡婊戠偣涓庡啿鍑绘垚鏈缓妯?6. 灏忚祫閲戝彲浜ゆ槗杩囨护锛堣兘鍚︿拱涓€鎵嬨€佽竟闄呮槸鍚﹁鐩栨垚鏈級

## 9. 杩愮淮銆佸憡璀︿笌瀹¤

### 9.1 杩愮淮浣滀笟

- 浣滀笟娉ㄥ唽銆佽Е鍙戙€佽皟搴︺€丼LA 妫€鏌ワ細`/ops/jobs/*`

### 9.2 鍛婅绯荤粺

- 璁㈤槄銆佸幓閲嶃€丄CK銆佸€肩彮鍥炶皟銆佸璐︼細`/alerts/*`

### 9.3 鍚堣璇佹嵁鍖?
- 瀵煎嚭銆佺鍚嶃€佸绛俱€佹牎楠岋細`/compliance/evidence/*`

### 9.4 瀹¤閾?
- 瀹¤鏌ヨ锛歚/audit/events`
- 閾炬牎楠岋細`/audit/verify-chain`

## 10. 鍏抽敭 API 鍒嗙粍绱㈠紩

### 10.1 绯荤粺涓庨壌鏉?
- `GET /system/config`
- `GET /system/auth/me`
- `GET /system/auth/permissions`

### 10.2 甯傚満涓庢暟鎹帴鍏?
- `GET /market/bars`
- `GET /market/intraday`
- `GET /market/calendar`
- `GET /market/tushare/capabilities`
- `POST /market/tushare/prefetch`

### 10.3 鏁版嵁娌荤悊涓庤鍙瘉

- `POST /data/quality/report`
- `POST /data/snapshots/register`
- `GET /data/snapshots`
- `POST /data/pit/validate`
- `POST /data/pit/validate-events`
- `POST /data/licenses/register`
- `GET /data/licenses`
- `POST /data/licenses/check`

### 10.4 浜嬩欢涓?NLP

- `POST /events/sources/register`
- `POST /events/ingest`
- `GET /events`
- `GET /events/connectors/overview`
- `POST /events/connectors/run`
- `GET /events/connectors/sla`
- `POST /events/nlp/normalize/ingest`
- `POST /events/nlp/drift-check`
- `GET /events/nlp/drift/slo/history`

### 10.5 鍥犲瓙銆佺瓥鐣ヤ笌娌荤悊

- `GET /factors/snapshot`
- `GET /strategies`
- `GET /strategies/{strategy_name}`
- `POST /strategy-governance/register`
- `POST /strategy-governance/submit-review`
- `POST /strategy-governance/decide`
- `GET /strategy-governance/policy`
- `POST /model-risk/drift-check`

### 10.6 淇″彿銆侀鎺т笌缁勫悎

- `POST /signals/generate`
- `POST /risk/check`
- `POST /portfolio/risk/check`
- `POST /portfolio/optimize`
- `POST /portfolio/rebalance/plan`
- `POST /portfolio/stress-test`

### 10.7 鍥炴祴銆佺爺绌躲€佽皟鍙備笌鎸戞垬璧?
- `POST /backtest/run`
- `POST /backtest/portfolio-run`
- `POST /research/run`
- `POST /pipeline/daily-run`
- `POST /autotune/run`
- `GET /autotune/profiles`
- `POST /autotune/profiles/rollback`
- `POST /challenge/run`

### 10.8 鎸佷粨涓庝笂绾块棴鐜?
- `POST /holdings/trades`
- `GET /holdings/trades`
- `GET /holdings/positions`
- `POST /holdings/analyze`
- `GET /reports/strategy-accuracy`
- `GET /reports/go-live-readiness`

### 10.9 鎵ц鍥炲啓涓庡鐩?
- `POST /replay/signals/record`
- `POST /replay/executions/record`
- `GET /replay/report`
- `GET /replay/attribution`
- `POST /replay/cost-model/calibrate`
- `GET /replay/cost-model/calibrations`

### 10.10 鎶ュ憡銆佸悎瑙勪笌璇佹嵁鍖?
- `POST /reports/generate`
- `POST /compliance/preflight`
- `POST /compliance/evidence/export`
- `POST /compliance/evidence/verify`
- `POST /compliance/evidence/countersign`

### 10.11 杩愮淮璋冨害涓庡憡璀?
- `GET /metrics/summary`
- `GET /metrics/ops-dashboard`
- `POST /ops/jobs/register`
- `POST /ops/jobs/{job_id}/run`
- `POST /ops/jobs/scheduler/tick`
- `GET /ops/jobs/scheduler/sla`
- `GET /ops/dashboard`
- `GET /alerts/recent`
- `POST /alerts/oncall/reconcile`

### 10.12 瀹¤

- `GET /audit/events`
- `GET /audit/export`
- `GET /audit/verify-chain`

## 11. 姣忔棩鎿嶄綔 SOP锛堝缓璁級

### 11.1 鐩樺墠

1. 鐪?`/ops/dashboard`锛岀‘璁ゆ棤 critical銆?2. 璺戞寫鎴樿禌鎴栫‘璁ゅ綋鍓嶅啝鍐涚敾鍍忎粛鐢熸晥銆?3. 鍒锋柊鎸佷粨鍒嗘瀽锛岀敓鎴愭鏃ュ缓璁€?4. 鍒锋柊涓婄嚎鍑嗗叆鎶ュ憡锛岀‘璁ゆ槸鍚﹀厑璁哥户缁伆搴︺€?
### 11.2 鐩樹腑

1. 鍙傝€冩寔浠撻〉鎺ㄨ崘鎵ц鏃舵銆?2. 鎵嬪伐涓嬪崟鍚庣珛鍗冲洖濉垚浜よ褰曘€?3. 鑻ヨЕ鍙戝洖婊氭潯浠讹紝鍋滄澧炰粨骞跺洖婊氱敾鍍忋€?
### 11.3 鐩樺悗

1. 鍥炲啓鎵ц骞跺埛鏂板鐩樺綊鍥犮€?2. 鍒锋柊鍑嗙‘鎬х湅鏉夸笌涓婄嚎鍑嗗叆鐪嬫澘銆?3. 璁板綍寮傚父涓庣浜屽ぉ淇鍔ㄤ綔銆?
## 12. 涓婄嚎鍑嗗叆寤鸿锛堝疄鐩樼伆搴︼級

寤鸿闂ㄦ锛堝彲鎸変綘鐨勯闄╁亸濂藉井璋冿級锛?
1. OOS 鏍锋湰鏁?`>= 40`
2. OOS 鍛戒腑鐜?`>= 55%`
3. Brier `<= 0.23`
4. 鎵ц瑕嗙洊鐜?`>= 70%`
5. 鎴愭湰鍚庡姩浣滄敹鐩?`>= 0`
6. 鎵ц璺熼殢鐜?`>= 65%`
7. 骞冲潎婊戠偣 `<= 35 bps`
8. 骞冲潎寤惰繜 `<= 1.2 澶ー
9. 鏈€杩?30 澶╁瓨鍦ㄦ寫鎴樿禌楠岃瘉缁撴灉

鑷姩鍥炴粴瑙﹀彂鍣紙绀轰緥锛夛細

1. 杩炵画 3 鏃ヤ簭鎹?2. 鍗曟棩缁勫悎鏀剁泭 `<= -2.5%`
3. 鐏板害绐楀彛鏈€澶у洖鎾?`> 6%`
4. 5 鏃ユ墽琛岃鐩栫巼 `< 60%` 鎴?5 鏃ュ钩鍧囨粦鐐?`> 45 bps`

## 13. 甯歌闂

1. 鏍囩椤电偣涓嶅姩
- 鍏堢湅娴忚鍣ㄦ帶鍒跺彴鏄惁鏈?JS 鎶ラ敊銆?- 寮哄埛缂撳瓨鍚庨噸璇曘€?- 纭 `/ui/trading-workbench/app.js` 鑳芥甯稿姞杞斤紙HTTP 200锛夈€?
2. 鏂囨。鎴栭〉闈㈠嚭鐜颁贡鐮?- 纭鏂囦欢淇濆瓨涓?UTF-8銆?- Windows 缁堢寤鸿浣跨敤 `chcp 65001`銆?
3. 鍚姩澶辫触鎻愮ず甯冨皵瑙ｆ瀽閿欒
- `.env` 涓嶈鍐?`true `锛堟湯灏剧┖鏍硷級锛屽繀椤绘槸 `true` 鎴?`false`銆?
4. 涓轰粈涔堟敹鐩婂拰鍥炴祴涓嶅悓
- 瀹炵洏鏈夋墽琛屽亸宸€佹粦鐐广€佸欢杩熶笌婕忓崟銆?- 璇蜂互鈥滄垚鏈悗鏀剁泭 + 鍑嗙‘鎬х湅鏉?+ 涓婄嚎鍑嗗叆鎶ュ憡鈥濈患鍚堝垽鏂€?
## 14. 缁撹

杩欎唤鎵嬪唽宸茬粡瑕嗙洊褰撳墠绯荤粺涓诲姛鑳介潰銆傝嫢鍚庣画鏂板妯″潡锛岃鍚屾鏇存柊鏈枃浠跺拰 `README.md` 鐨勨€滆兘鍔涜寖鍥粹€濅笌鈥淎PI 鎬昏鈥濄€?

