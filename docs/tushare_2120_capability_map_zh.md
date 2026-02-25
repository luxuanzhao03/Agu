# Tushare 2120 绉垎鑳藉姏鏄犲皠锛堢郴缁熸帴鍏ョ増锛?
鏈枃妗ｇ敤浜庤鏄庯細鍦?`Tushare 2120` 绉垎涓嬶紝绯荤粺褰撳墠濡備綍璇嗗埆鍙敤鏁版嵁绉嶇被銆佸摢浜涘凡缁忔帴鍏ョ瓥鐣ヤ富閾捐矾銆佸浣曚竴閿鍙栥€?
## 1. 褰撳墠鎺ュ叆鍘熷垯

- 鏁版嵁浼樺厛绾э細`tushare -> akshare`锛堝け璐ヨ嚜鍔ㄥ洖閫€锛夈€?- 绉垎闂ㄦ锛氭寜 `min_points_hint` 璁＄畻鍙敤鎬э紝2120 榛樿瑕嗙洊 2000 妗ｆ暟鎹紝涓嶈鐩?5000 妗ｏ紙渚嬪 `bak_daily`锛夈€?- 鎺ュ彛鍙敤鎬э細鍚屾椂妫€鏌ユ湰鍦?`pro_api` 鏄惁瀛樺湪瀵瑰簲鏂规硶锛坄api_available`锛夈€?- 绛栫暐鎺ュ叆鏍囧噯锛氬彧鏈夆€滄棩棰戝彲瀵归綈 + 涓庝氦鏄撳喅绛栫洿鎺ョ浉鍏斥€濈殑瀛楁鎵嶈繘鍏ュ洜瀛愬紩鎿庡疄鏃舵墦鍒嗐€?
## 2. 鏁版嵁闆嗚兘鍔涚洰褰曪紙2120 瑙嗚锛?
### 2.1 宸叉帴鍏ョ瓥鐣ヤ富閾捐矾锛堣嚜鍔ㄨ瀺鍚堝埌 `/market/bars` 鏃ョ嚎锛?
1. `daily_basic`锛?000锛?- 鍏抽敭瀛楁锛歚turnover_rate`, `turnover_rate_f`, `volume_ratio`, `pe_ttm`, `pb`, `ps_ttm`, `dv_ttm`, `circ_mv`
- 绯荤粺瀛楁锛歚ts_turnover_rate*`, `ts_pe_ttm`, `ts_pb`, `ts_ps_ttm`, `ts_dv_ttm`, `ts_circ_mv`
- 鐢ㄩ€旓細浼板€艰瘎鍒?+ 鍙氦鏄撴€ц瘎鍒?
2. `moneyflow`锛?000锛?- 鍏抽敭瀛楁锛歚net_mf_amount`, `buy_elg_amount`, `sell_elg_amount`, `buy_lg_amount`, `sell_lg_amount`
- 绯荤粺瀛楁锛歚ts_net_mf_amount`, `ts_main_net_mf_amount`, `ts_buy_elg_amount`, `ts_sell_elg_amount`
- 鐢ㄩ€旓細璧勯噾娴佽瘎鍒?+ 涔板叆渚х‘璁?
3. `stk_limit`锛?000锛?- 鍏抽敭瀛楁锛歚up_limit`, `down_limit`
- 绯荤粺瀛楁锛歚ts_up_limit`, `ts_down_limit`
- 鐢ㄩ€旓細鍙氦鏄撶┖闂磋瘎鍒嗐€佹定璺屽仠杈圭晫鎰熺煡

4. `adj_factor`锛?000锛?- 鍏抽敭瀛楁锛歚adj_factor`
- 绯荤粺瀛楁锛歚ts_adj_factor`
- 鐢ㄩ€旓細澶嶆潈涓€鑷存€ц緟鍔╂鏌?
5. `fina_indicator` / `income` / `balancesheet` / `cashflow`锛?000锛?- 鐢ㄩ€旓細璐㈡姤蹇収涓庡熀鏈潰璇勫垎閾捐矾
- 璇存槑锛氱敱鍩烘湰闈㈠寮烘湇鍔＄粺涓€鎺ュ叆锛屼笉鐩存帴浣滀负鏃ョ嚎 merge 瀛楁銆?
### 2.2 宸茬撼鍏ヨ兘鍔涚洰褰曚笌鎵归噺棰勫彇锛堟殏鏈洿鎺ヨ繘鍏ュ疄鏃剁瓥鐣ユ墦鍒嗭級

- `forecast`, `express`, `dividend`, `fina_audit`
- `top10_holders`, `top10_floatholders`, `stk_holdernumber`
- `pledge_stat`, `pledge_detail`, `repurchase`, `share_float`, `block_trade`

鐢ㄩ€斿畾浣嶏細
- 浜嬩欢娌荤悊/NLP锛氬叕鍛婅Е鍙戙€佹儏缁慨姝ｃ€侀闄╀簨浠舵爣绛俱€?- 鐮旂┒鍒嗘瀽锛氳偂涓滅粨鏋勫彉鍖栥€佽川鎶奸闄┿€佸叕鍙歌涓恒€?- 鍚堣涓庡鐩橈細棰勫彇鐣欑棔锛屼究浜庤瘉鎹寘涓庡璁¤拷婧€?
### 2.3 2120 涓嬮粯璁や笉鍙敤锛堣兘鍔涚洰褰曚細鏍囪锛?
- `bak_daily`锛?000锛?
## 3. 鏂板绯荤粺 API锛堝彲鐩存帴鍦ㄥ墠绔垨鑴氭湰璋冪敤锛?
1. 鏌ヨ鑳藉姏鐩綍锛?
```bash
curl "http://127.0.0.1:8000/market/tushare/capabilities?user_points=2120"
```

杩斿洖閲嶇偣瀛楁锛?- `eligible`: 绉垎鏄惁婊¤冻
- `api_available`: 鏈湴 pro_api 鏄惁鍏峰璇ユ柟娉?- `ready_to_call`: 褰撳墠鏄惁鍙洿鎺ヨ皟鐢?- `integrated_in_system`: 鏄惁宸叉帴鍏ョ瓥鐣ヤ富閾捐矾
- `integrated_targets`: 鎺ュ叆妯″潡娓呭崟

2. 鎵归噺棰勫彇锛堥€愭暟鎹泦鐘舵€侊級锛?
```bash
curl -X POST "http://127.0.0.1:8000/market/tushare/prefetch" \
  -H "Content-Type: application/json" \
  -d "{\"symbol\":\"000001\",\"start_date\":\"2025-01-01\",\"end_date\":\"2025-12-31\",\"user_points\":2120,\"include_ineligible\":false}"
```

杩斿洖閲嶇偣瀛楁锛?- `summary.success/failed/skipped`
- `results[].status`锛坄success|failed|skipped_ineligible|skipped_api_unavailable`锛?- `results[].used_params`锛堝疄闄呰皟鐢ㄥ弬鏁帮級
- `results[].row_count/column_count`

## 4. 绛栫暐涓庡洜瀛愭帴鍏ョ粨鏋?
1. 鍥犲瓙寮曟搸鏂板锛?- `tushare_valuation_score`
- `tushare_moneyflow_score`
- `tushare_tradability_score`
- `tushare_advanced_score`
- `tushare_advanced_completeness`

2. 绛栫暐鍗囩骇锛?- `multi_factor`锛氭柊澧?`w_tushare_advanced` 涓?`min_tushare_score_buy`

3. 鍏煎鎬э細
- 鑻ラ珮绾у瓧娈电己澶憋紝涓嶄細鎶ラ敊锛涢粯璁ゅ洖閫€鍒颁腑鎬у垎锛?.5锛夛紝纭繚涓绘祦绋嬬ǔ瀹氥€?
## 5. 鎺ㄨ崘浣跨敤娴佺▼

1. 鍦?`.env` 璁剧疆锛歚DATA_PROVIDER_PRIORITY=tushare,akshare` 骞跺～鍐?`TUSHARE_TOKEN`銆?2. 璋冪敤 `/market/tushare/capabilities` 纭 2120 涓嬬殑 `ready_to_call`銆?3. 瀵归噸鐐硅偂绁ㄥ厛璋冪敤 `/market/tushare/prefetch` 鍋氶鐑笌鍙敤鎬ф鏌ャ€?4. 鍐嶈繍琛?`/signals/generate` / `/research/run` / `/backtest/run`锛岃瀵?`tushare_advanced_score` 鐨勫奖鍝嶃€?
