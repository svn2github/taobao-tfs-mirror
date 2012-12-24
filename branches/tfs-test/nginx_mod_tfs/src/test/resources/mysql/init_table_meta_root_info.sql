USE tfs_stat;

/*
Navicat MySQL Data Transfer

Source Server         : 10.232.4.29(ngnix_rc_db)
Source Server Version : 50077
Source Host           : 10.232.4.29:3306
Source Database       : tfs_stat

Target Server Type    : MYSQL
Target Server Version : 50077
File Encoding         : 65001

Date: 2012-11-02 13:20:10
*/

SET FOREIGN_KEY_CHECKS=0;
-- ----------------------------
-- Table structure for `t_meta_root_info`
-- ----------------------------
DROP TABLE IF EXISTS `t_meta_root_info`;
CREATE TABLE `t_meta_root_info` (
  `app_id` int(11) NOT NULL default '0',
  `addr_info` varchar(64) NOT NULL,
  `stat` int(11) NOT NULL default '1',
  `rem` varchar(255) default NULL,
  `create_time` datetime default NULL,
  `modify_time` datetime default NULL,
  PRIMARY KEY  (`app_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

-- ----------------------------
-- Records of t_meta_root_info
-- ----------------------------
INSERT INTO `t_meta_root_info` VALUES ('1', '10.232.36.202:8866', '1', '1', NOW(), NOW());
INSERT INTO `t_meta_root_info` VALUES ('2', '10.232.36.202:8866', '1', '2', NOW(), NOW());
INSERT INTO `t_meta_root_info` VALUES ('3', '10.232.36.202:8866', '1', '3', NOW(), NOW());
INSERT INTO `t_meta_root_info` VALUES ('4', '10.232.36.202:8866', '1', '4', NOW(), NOW());
INSERT INTO `t_meta_root_info` VALUES ('5', '10.232.36.202:8866', '1', '5', NOW(), NOW());
INSERT INTO `t_meta_root_info` VALUES ('6', '10.232.36.202:8866', '1', '6', NOW(), NOW());
