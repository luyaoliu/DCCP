WITH brick_name AS (
SELECT distinct EUCLID_BRICK_EVEN_SIDE_ID PORT_CONT_PARENT_ID, BRICK_HOST_NAME
FROM INFAUTO_DDL.O_INFR_DLY_RDPM_BR_HOST_MP
UNION 
SELECT distinct EUCLID_BRICK_ODD_SIDE_ID PORT_CONT_PARENT_ID, BRICK_HOST_NAME
FROM INFAUTO_DDL.O_INFR_DLY_RDPM_BR_HOST_MP),

brick AS(
SELECT 
SNAPSHOT_DAY,
pp.PORT_CONT_PARENT_ID,
pp.SITE,
ROOM,
ROW_NBR,
POSITION_NUMBER,
BRICK_HOST_NAME
FROM INFAUTO_DDL.O_INFR_DLY_RDPM_PORT_CONT_PRT pp
LEFT JOIN brick_name
ON pp.PORT_CONT_PARENT_ID = brick_name.PORT_CONT_PARENT_ID
),

brick_position_id AS (
select ID as position_id, Name as position_name, rack_position_alias, 
concat(substr(physical_site_code,1,3),to_char(substr(physical_site_code,4),'FM000')) as site, room_name, date_start
from INFAUTO_DDL.o_infr_dly_rack_position rp
JOIN INFAUTO_DDL.D_INFR_ROOM_DIM rm ON rm.room_asset_id = rp.room_asset_id
where snapshot_day = (select max(snapshot_day) from INFAUTO_DDL.o_infr_dly_rack_position)
and rack_position_type = 'NETWORK'),

port_status AS(
SELECT 
p.SNAPSHOT_DAY,
brick.BRICK_HOST_NAME,
brick.SITE,
brick.ROOM,
brick_position_id.position_id,
brick_position_id.date_start,
CASE WHEN instr(brick.BRICK_HOST_NAME,'-es-',1,1)>0 THEN 'EC2'
ELSE 'production' END as FABRIC,
port_id,
STATUS
FROM INFAUTO_DDL.O_INFR_DLY_RDPM_PORT p
LEFT JOIN INFAUTO_DDL.O_INFR_DLY_RDPM_PORT_CONT c
ON p.SNAPSHOT_DAY = c.SNAPSHOT_DAY
AND p.PORT_CONTAINER_ID = c.PORT_CONT_ID
LEFT JOIN brick
ON c.SNAPSHOT_DAY = brick.SNAPSHOT_DAY
AND c.PORT_CONT_PARENT_ID = brick.PORT_CONT_PARENT_ID
LEFT JOIN brick_position_id
ON brick.SITE = brick_position_id.SITE 
AND brick.ROOM = brick_position_id.ROOM_name 
AND brick.position_number = brick_position_id.position_name
WHERE c.CONTAINER_TYPE = 'EuclidBrickRouter' 
AND p.SNAPSHOT_DAY = (SELECT MAX(SNAPSHOT_DAY) FROM INFAUTO_DDL.O_INFR_DLY_RDPM_PORT)
),

connection_status AS
(SELECT 
sv_position.SNAPSHOT_DAY,
brick_position.brick_host_name,
svbr_map.RACK_TYPE,
svbr_map.RACK_CATEGORY,
sv_position.UPLINK_CONFIG,
CASE
WHEN instr(sv_position.UPLINK_CONFIG,'+',1,1)=0
THEN TO_NUMBER(regexp_substr(sv_position.UPLINK_CONFIG,'\((.*?)\)',1,1,null,1))
WHEN instr(sv_position.UPLINK_CONFIG,'+',1,2)=0
THEN TO_NUMBER(substr(sv_position.UPLINK_CONFIG,2,instr(sv_position.UPLINK_CONFIG,'+',1,1)-2))
+TO_NUMBER(substr(sv_position.UPLINK_CONFIG,instr(sv_position.UPLINK_CONFIG,'+',1,1)+1,instr(sv_position.UPLINK_CONFIG,')',1,1)-1-instr(sv_position.UPLINK_CONFIG,'+',1,1)))
WHEN instr(sv_position.UPLINK_CONFIG,'+',1,3)=0
THEN TO_NUMBER(substr(sv_position.UPLINK_CONFIG,2,instr(sv_position.UPLINK_CONFIG,'+',1,1)-2))
+TO_NUMBER(substr(sv_position.UPLINK_CONFIG,instr(sv_position.UPLINK_CONFIG,'+',1,1)+1,instr(sv_position.UPLINK_CONFIG,'+',1,2)-1-instr(sv_position.UPLINK_CONFIG,'+',1,1)))
+TO_NUMBER(substr(sv_position.UPLINK_CONFIG,instr(sv_position.UPLINK_CONFIG,'+',1,2)+1,instr(sv_position.UPLINK_CONFIG,')',1,1)-1-instr(sv_position.UPLINK_CONFIG,'+',1,2)))
ELSE TO_NUMBER(substr(sv_position.UPLINK_CONFIG,2,instr(sv_position.UPLINK_CONFIG,'+',1,1)-2))
+TO_NUMBER(substr(sv_position.UPLINK_CONFIG,instr(sv_position.UPLINK_CONFIG,'+',1,1)+1,instr(sv_position.UPLINK_CONFIG,'+',1,2)-1-instr(sv_position.UPLINK_CONFIG,'+',1,1)))
+TO_NUMBER(substr(sv_position.UPLINK_CONFIG,instr(sv_position.UPLINK_CONFIG,'+',1,2)+1,instr(sv_position.UPLINK_CONFIG,'+',1,3)-1-instr(sv_position.UPLINK_CONFIG,'+',1,2)))
+TO_NUMBER (substr(sv_position.UPLINK_CONFIG,instr(sv_position.UPLINK_CONFIG,'+',1,3)+1,instr(sv_position.UPLINK_CONFIG,')',1,1)-1-instr(sv_position.UPLINK_CONFIG,'+',1,3)))
END AS uplink_count,
sv_position.CONNECTION_STATE
FROM INFAUTO_DDL.O_INFR_DLY_RDPM_SV_RC_POSITION sv_position
LEFT JOIN INFAUTO_DDL.O_INFR_DLY_RDPM_SRVR_POS_BR_MP svbr_map
ON sv_position.SNAPSHOT_DAY = svbr_map.SNAPSHOT_DAY
AND sv_position.ID = svbr_map.SERVER_RACK_POSITION_ID
LEFT JOIN INFAUTO_DDL.O_INFR_DLY_RDPM_BR_HOST_MP brick_position
ON svbr_map.SNAPSHOT_DAY=brick_position.SNAPSHOT_DAY
AND svbr_map.BRICK_POS_HOSTNAME_MAP_ID = brick_position.id
WHERE sv_position.SNAPSHOT_DAY = (select max(SNAPSHOT_DAY) from INFAUTO_DDL.O_INFR_DLY_RDPM_SV_RC_POSITION)
),

port_view AS (
Select * from
(select SNAPSHOT_DAY,SITE, ROOM, FABRIC, brick_host_name,port_id,status, date_start AS START_DATE
from port_status)
PIVOT (count(port_id) AS port 
FOR status IN ('IN_USE' AS TOTAL_IN_USE, 'FREE' AS FREE_NON_STORAGE ,'STORAGE' AS RESERVED_STORAGE))),

pivot_connection_status AS(
select * from
(select brick_host_name, 
CASE WHEN rack_category = 'STORAGE_RACK'
THEN 'STORAGE'
ELSE 'NON_STORAGE' END AS Category, 
uplink_count, connection_state 
from connection_status 
)
PIVOT (sum(uplink_count) AS port 
FOR (connection_state, CATEGORY) 
IN (
('CABLED','STORAGE')AS CABLED_STORAGE,
('CABLED','NON_STORAGE') AS CABLED_NON_STORAGE,
('CABLING_IN_PROGRESS','STORAGE') AS CABLING_STORAGE,
('CABLING_IN_PROGRESS','NON_STORAGE')AS CABLING_NON_STORAGE
)))

SELECT
port_view.SNAPSHOT_DAY,
port_view.BRICK_HOST_NAME,
port_view.START_DATE,
port_view.SITE,
port_view.ROOM,
port_view.FABRIC,
port_view.TOTAL_IN_USE_PORT + port_view.FREE_NON_STORAGE_PORT + port_view.RESERVED_STORAGE_PORT as TOTAL_PORT,
port_view.TOTAL_IN_USE_PORT,
CASE WHEN CABLED_NON_STORAGE_PORT is NOT NULL
THEN CABLED_NON_STORAGE_PORT ELSE 0 END AS CABLED_NON_STORAGE_PORT,
CASE WHEN CABLED_STORAGE_PORT is NOT NULL
THEN CABLED_STORAGE_PORT ELSE 0 END AS CABLED_STORAGE_PORT,
CASE WHEN CABLING_NON_STORAGE_PORT is NOT NULL
THEN CABLING_NON_STORAGE_PORT ELSE 0 END AS CABLING_NON_STORAGE_PORT,
CASE WHEN CABLING_STORAGE_PORT is NOT NULL
THEN CABLING_STORAGE_PORT ELSE 0 END AS CABLING_STORAGE_PORT,
port_view.FREE_NON_STORAGE_PORT + port_view.RESERVED_STORAGE_PORT AS TOTAL_FREE_PORT,
port_view.FREE_NON_STORAGE_PORT,
port_view.RESERVED_STORAGE_PORT
FROM port_view
LEFT JOIN pivot_connection_status 
ON port_view.BRICK_HOST_NAME = pivot_connection_status.BRICK_HOST_NAME
