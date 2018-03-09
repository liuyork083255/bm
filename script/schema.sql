CREATE SCHEMA `wf_bond_monitor` DEFAULT CHARACTER SET utf8mb4 ;
USE wf_bond_monitor;

CREATE TABLE `alerts` (
  `auto_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增序列',
  `id` varchar(64) NOT NULL DEFAULT '' COMMENT '主键id',
  `description` varchar(32) DEFAULT NULL COMMENT '估值偏离描述',
  `update_time` time DEFAULT NULL COMMENT '更新时间',
  `name` varchar(20) DEFAULT NULL COMMENT '发行机构',
  `bond_type` varchar(4) DEFAULT NULL COMMENT '债券类型',
  `issuer_name` varchar(32) DEFAULT NULL COMMENT '机构名称',
  `alert_type` varchar(2) DEFAULT NULL COMMENT '监控类型',
  `bond_key_listed_market` varchar(32) DEFAULT NULL COMMENT '债券key+交易市场',
  PRIMARY KEY (`auto_id`),
  UNIQUE KEY `unique_index` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `bond_infos` (
  `auto_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增序列',
  `bond_key` varchar(32) NOT NULL DEFAULT '' COMMENT '债券key',
  `listed_market` varchar(4) NOT NULL DEFAULT '' COMMENT '交易市场',
  `yield_curve_code` varchar(32) DEFAULT NULL COMMENT '评级全称',
  `category` varchar(15) DEFAULT NULL COMMENT '分类',
  `sub_category` varchar(15) DEFAULT NULL COMMENT '子分类',
  `rating` varchar(10) DEFAULT NULL COMMENT '评级',
  `issuer_code` varchar(15) DEFAULT NULL COMMENT '机构代码',
  `bond_type` varchar(2) DEFAULT NULL COMMENT 'C-信用债,R-利率债',
  `issuer_name` varchar(50) DEFAULT NULL COMMENT '机构名称',
  `short_name` varchar(32) DEFAULT NULL COMMENT '债券简称',
  `bond_sub_type` varchar(3) DEFAULT NULL COMMENT '债券子类型',
  `is_municipal` varchar(1) DEFAULT NULL COMMENT '是否城投',
  `listed_type` varchar(18) DEFAULT NULL COMMENT '上市类型',
  `institution_subtype` varchar(5) DEFAULT NULL COMMENT '机构子类型',
  `cbrc_financing_platform` varchar(5) DEFAULT NULL COMMENT '平台债',
  `province` varchar(5) DEFAULT NULL COMMENT '省份',
  PRIMARY KEY (`auto_id`),
  UNIQUE KEY `unique_index` (`bond_key`,`listed_market`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `credit_bond_stats` (
  `auto_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增序列',
  `issuer` varchar(32) NOT NULL DEFAULT '' COMMENT '机构代码',
  `bid_fif` int DEFAULT NULL COMMENT '15min bid量',
  `ofr_fif` int DEFAULT NULL COMMENT '15min ofr量',
  `trade_fif` int DEFAULT NULL COMMENT '15min trade量',
  `bid_acc` int DEFAULT NULL COMMENT 'bid累积量',
  `ofr_acc` int DEFAULT NULL COMMENT 'ofr累积量',
  `trade_acc` int DEFAULT NULL COMMENT 'trade累积量',
  `bid_week_avg` decimal(16,4) DEFAULT NULL COMMENT 'bid周平均统计',
  `ofr_week_avg` decimal(16,4) DEFAULT NULL COMMENT 'ofr周平均统计',
  `trade_week_avg` decimal(16,4) DEFAULT NULL COMMENT 'trade周平均统计',
  `bond_count` int DEFAULT NULL COMMENT '债券数量',
  `active_count` int DEFAULT NULL,
  `active_bond` varchar(15) DEFAULT NULL,
  PRIMARY KEY (`auto_id`),
  UNIQUE KEY `unique_index` (`issuer`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `quote_by_bond_history_stats` (
  `auto_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增序列',
  `bond_key` varchar(32) DEFAULT NULL COMMENT '债券key',
  `listed_market` varchar(3) DEFAULT NULL COMMENT '交易市场',
  `quote_type` int(11) NOT NULL DEFAULT '0' COMMENT '报价类型',
  `total` decimal(16,4) DEFAULT NULL COMMENT '总量',
  PRIMARY KEY (`auto_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `quote_days` (
  `auto_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增序列',
  `day` date DEFAULT NULL COMMENT '日期',
  `start_time` varchar(5) DEFAULT NULL COMMENT '小时:分钟',
  `quote_type` int DEFAULT NULL COMMENT '报价类型',
  `total` int DEFAULT NULL COMMENT '总量',
  PRIMARY KEY (`auto_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `quote_histories` (
  `auto_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增序列',
  `quote_time` datetime DEFAULT NULL COMMENT '报价日期',
  `quote_type` int DEFAULT NULL COMMENT '报价类型',
  `bond_key` varchar(32) DEFAULT NULL COMMENT '债券key',
  `listed_market` varchar(3) DEFAULT NULL COMMENT '交易市场',
  `remain_time` varchar(25) DEFAULT NULL COMMENT '剩余期限',
  `remain_time_value` int DEFAULT NULL COMMENT '剩余期限总数',
  PRIMARY KEY (`auto_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `quote_history_originals` (
  `auto_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增序列',
  `quote_time` datetime DEFAULT NULL COMMENT '报价日期',
  `quote_type` int DEFAULT NULL COMMENT '报价类型',
  `bond_key` varchar(32) DEFAULT NULL COMMENT '债券key',
  `listed_market` varchar(3) DEFAULT NULL COMMENT '交易市场',
  `remain_time` varchar(25) DEFAULT NULL COMMENT '剩余期限',
  `remain_time_value` int DEFAULT NULL COMMENT '剩余期限总量',
  `id` varchar(34) DEFAULT NULL COMMENT '对应源行情的primary key，quote-uuid，trade-tradeId+brokerId',
  `trans_type` varchar(15) DEFAULT NULL COMMENT '成交类型 insert update delete',
--   `uuid` varchar(32) DEFAULT NULL COMMENT '对应源数据的主键',
  PRIMARY KEY (`auto_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `quote_history_stats` (
  `auto_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增序列',
  `start_time` varchar(5) NOT NULL DEFAULT '' COMMENT '开始时间 时:分',
  `quote_type` int NOT NULL DEFAULT '0' COMMENT '报价类型',
  `total` decimal(16,4) DEFAULT NULL COMMENT '总量',
  PRIMARY KEY (`auto_id`),
  UNIQUE KEY `unique_index` (`start_time`,`quote_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 这个表是二期项目，原来设计是longblob类型，后面会拆分，一期上线不会创建
CREATE TABLE `quote_managers` (
  `auto_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增序列',
  `day` varchar(100) NOT NULL DEFAULT '',
  `ranges` longblob,
  `hot_maps` longblob,
  `acc_quote_maps` longblob,
  `acc_alert_maps` longblob,
  PRIMARY KEY (`auto_id`),
  UNIQUE KEY `unique_index` (`day`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `quotes` (
  `auto_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增序列',
  `id` varchar(32) NOT NULL DEFAULT '' COMMENT '成交id',
  `quote_type` int NOT NULL DEFAULT '0' COMMENT '报价类型',
  `bond_key` varchar(32) DEFAULT NULL COMMENT '债券key',
  `listed_market` varchar(3) DEFAULT NULL COMMENT '交易市场',
  `quote_time` time DEFAULT NULL COMMENT '报价时间',
  `rate` varchar(10) DEFAULT NULL COMMENT '评级',
  `remain_time_value` int DEFAULT NULL COMMENT '剩余期限',
  `remain_time` varchar(15) DEFAULT NULL COMMENT '格式化 剩余期限',
  `issuer_code` varchar(32) DEFAULT NULL COMMENT '机构代码',
  `term` varchar(15) DEFAULT NULL COMMENT '剩余期限',
  PRIMARY KEY (`auto_id`),
  UNIQUE KEY `unique_index` (`id`,`quote_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `trade_stats` (
  `auto_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增序列',
  `bond_code` varchar(25) NOT NULL DEFAULT '' COMMENT '债券代码',
  `bond_key` varchar(32) DEFAULT NULL COMMENT '债券key',
  `listed_market` varchar(3) DEFAULT NULL COMMENT '交易市场',
  `short_name` varchar(32) DEFAULT NULL COMMENT '债券简称',
  `cdc_price` decimal(16,4) DEFAULT NULL COMMENT '中债估值',
  `avg_diff_price` decimal(16,4) DEFAULT NULL COMMENT '当日平均价格',
  `latest_price` decimal(16,4) DEFAULT NULL COMMENT '最新价格',
  `latest_price_diff` decimal(16,4) DEFAULT NULL COMMENT '最新价格',
  `price_diff1_count` int DEFAULT NULL COMMENT '成交-中债 bp',
  `price_diff2_count` int DEFAULT NULL COMMENT '成交-中债 bp',
  `price_diff3_count` int DEFAULT NULL COMMENT '成交-中债 bp',
  `price_diff4_count` int DEFAULT NULL COMMENT '成交-中债 bp',
  `price_diff5_count` int DEFAULT NULL COMMENT '成交-中债 bp',
  `price_diff6_count` int DEFAULT NULL COMMENT '成交-中债 bp',
  `price_diff7_count` int DEFAULT NULL COMMENT '成交-中债 bp',
  `price_diff8_count` int DEFAULT NULL COMMENT '成交-中债 bp',
  `price_diff9_count` int DEFAULT NULL COMMENT '成交-中债 bp',
  `price_diff10_count` int DEFAULT NULL COMMENT '成交-中债 bp',
  `update_time` time DEFAULT NULL COMMENT '更新时间',
  `yield_curve` varchar(20) DEFAULT NULL COMMENT '评级',
  `trade_count` int DEFAULT NULL COMMENT '成交数量',
  PRIMARY KEY (`auto_id`),
  UNIQUE KEY `unique_index` (`bond_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `trades` (
  `auto_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增序列',
  `id` varchar(50) NOT NULL DEFAULT '',
  `bond_key` varchar(32) DEFAULT NULL COMMENT '债券key',
  `bond_code` varchar(18) DEFAULT NULL COMMENT '债券代码',
  `listed_market` varchar(3) DEFAULT NULL COMMENT '交易市场',
  `short_name` varchar(20) DEFAULT NULL COMMENT '债券简称',
  `deal_price` varchar(10) DEFAULT NULL COMMENT '成交价格',
  `deal_price_value` decimal(16,4) DEFAULT NULL COMMENT '格式化 成交价格',
  `cdc_valuation` varchar(10) DEFAULT NULL COMMENT '中债估值',
  `cdc_valuation_value` decimal(16,4) DEFAULT NULL COMMENT '格式化 中债估值',
  `cdc_diff` varchar(10) DEFAULT NULL COMMENT '估值偏离',
  `cdc_diff_value` decimal(16,4) DEFAULT NULL COMMENT '格式化 估值偏离',
  `update_time` time DEFAULT NULL COMMENT '更新时间',
  `remain_time` varchar(32) DEFAULT NULL COMMENT '剩余期限',
  `yield_curve` varchar(30) DEFAULT NULL COMMENT '评级',
  PRIMARY KEY (`auto_id`),
  UNIQUE KEY `unique_index` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 这个表只有几十行记录，为固定值，不需要自增
CREATE TABLE `yield_curves` (
  `code` varchar(30) NOT NULL DEFAULT '' COMMENT '评级code',
  `name` varchar(32) DEFAULT NULL COMMENT '机构曲线名称',
  PRIMARY KEY (`code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;