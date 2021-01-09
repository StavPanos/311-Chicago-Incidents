package com.m149.persistence;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author stavropoulosp
 */
public class IncidentsPersistence {

  private JdbcFetch jdbcFetch = new JdbcFetch();

  public int selectCount(Map criteria, int queryNumber) {
    int rowsCount = 0;

    if (queryNumber != 7 && queryNumber != 8 && (criteria == null || criteria.isEmpty())) {
      return 0;
    }

    switch (queryNumber) {
      case (1):
        rowsCount = jdbcFetch.selectCount(getSql1(criteria), getSqlParameters1(criteria));
        break;
      case (2):
        rowsCount = jdbcFetch.selectCount(getSql2(criteria), getSqlParameters2(criteria));
        break;
      case (3):
        rowsCount = jdbcFetch.selectCount(getSql3(criteria), getSqlParameters3(criteria));
        break;
      case (4):
        rowsCount = jdbcFetch.selectCount(getSql4(criteria), getSqlParameters4(criteria));
        break;
      case (5):
        rowsCount = jdbcFetch.selectCount(getSql5(criteria), getSqlParameters5(criteria));
        break;
      case (6):
        rowsCount = jdbcFetch.selectCount(getSql6(criteria), getSqlParameters6(criteria));
        break;
      case (7):
        rowsCount = jdbcFetch.selectCount(getSql7(criteria), getSqlParameters7(criteria));
        break;
      case (8):
        rowsCount = jdbcFetch.selectCount(getSql8(criteria), getSqlParameters8(criteria));
        break;
      case (9):
        rowsCount = jdbcFetch.selectCount(getSql9(criteria), getSqlParameters9(criteria));
        break;
      case (10):
        rowsCount = jdbcFetch.selectCount(getSql10(criteria), getSqlParameters10(criteria));
        break;
      case (11):
        rowsCount = jdbcFetch.selectCount(getSql11(criteria), getSqlParameters11(criteria));
        break;
      case (12):
        rowsCount = jdbcFetch.selectCount(getSql12(criteria), getSqlParameters12(criteria));
        break;
      default:
        rowsCount = jdbcFetch.selectCount(getSql(criteria), getSqlParameters(criteria));
    }

    return rowsCount;
  }

  public List<Map> lazySelect(Map criteria, int page, int pageSize, int queryNumber) {

    List<Map> listTable = new ArrayList<Map>();

    if (queryNumber != 7 && queryNumber != 8 && (criteria == null || criteria.isEmpty())) {
      return new ArrayList<Map>();
    }

    switch (queryNumber) {
      case (1):
        listTable = jdbcFetch.select(getSql1(criteria), getSqlParameters1(criteria), page, pageSize);
        break;
      case (2):
        listTable = jdbcFetch.select(getSql2(criteria), getSqlParameters2(criteria), page, pageSize);
        break;
      case (3):
        listTable = jdbcFetch.select(getSql3(criteria), getSqlParameters3(criteria), page, pageSize);
        break;
      case (4):
        listTable = jdbcFetch.select(getSql4(criteria), getSqlParameters4(criteria), page, pageSize);
        break;
      case (5):
        listTable = jdbcFetch.select(getSql5(criteria), getSqlParameters5(criteria), page, pageSize);
        break;
      case (6):
        listTable = jdbcFetch.select(getSql6(criteria), getSqlParameters6(criteria), page, pageSize);
        break;
      case (7):
        listTable = jdbcFetch.select(getSql7(criteria), getSqlParameters7(criteria), page, pageSize);
        break;
      case (8):
        listTable = jdbcFetch.select(getSql8(criteria), getSqlParameters8(criteria), page, pageSize);
        break;
      case (9):
        listTable = jdbcFetch.select(getSql9(criteria), getSqlParameters9(criteria), page, pageSize);
        break;
      case (10):
        listTable = jdbcFetch.select(getSql10(criteria), getSqlParameters10(criteria), page, pageSize);
        break;
      case (11):
        listTable = jdbcFetch.select(getSql11(criteria), getSqlParameters11(criteria), page, pageSize);
        break;
      case (12):
        listTable = jdbcFetch.select(getSql12(criteria), getSqlParameters12(criteria), page, pageSize);
        break;
      default:
        listTable = jdbcFetch.select(getSql(criteria), getSqlParameters(criteria), page, pageSize);
    }

    return listTable;
  }

  public String getSql1(Map cretiria) {

    StringBuilder sqlPecs = new StringBuilder("select r.DESCRIPTION, count(*) REQUESTS\n"
            + "from \"M149\".\"INCIDENT\" i, \"M149\".\"REQUEST_TYPE\" r\n"
            + "where i.SERVICE_REQUEST_TYPE = r.CODE\n"
            + "and CREATION_DATE between to_date(?, 'yyyy/mm/dd')\n"
            + "and to_date(?, 'yyyy/mm/dd') group by r.DESCRIPTION order by REQUESTS desc");

    return sqlPecs.toString();
  }

  public String getSql2(Map cretiria) {

    StringBuilder sqlPecs = new StringBuilder("select creation_date, count(*) TOTAL_REQUESTS from \"M149\".\"INCIDENT\"\n"
            + "where service_request_type = ?\n"
            + "and creation_date between to_date(?, 'yyyy/mm/dd')\n"
            + "and to_date(?, 'yyyy/mm/dd')\n"
            + "group by creation_date\n"
            + "order by creation_date desc");

    return sqlPecs.toString();
  }

  public String getSql3(Map cretiria) {
    StringBuilder sqlPecs = new StringBuilder("select r.description, i.zip_code, count(*) requests from \"M149\".\"INCIDENT\" i, \"M149\".\"REQUEST_TYPE\" r\n"
            + "where i.creation_date = to_date(?, 'yyyy/mm/dd')\n"
            + " and i.zip_code != '0'\n"
            + "group by r.description, i.zip_code\n"
            + "order by r.description, i.zip_code");

    return sqlPecs.toString();
  }

  public String getSql4(Map cretiria) {

    StringBuilder sqlPecs = new StringBuilder("select service_request_type, ROUND(CAST(float8 ((sum(EXTRACT(DAY FROM (completion_date - creation_date)))) / (count(*))) as numeric), 2) AVG_COMPL_TIME\n"
            + "from \"M149\".\"INCIDENT\"\n"
            + "where creation_date between to_date(?, 'yyyy/mm/dd')\n"
            + "and to_date(?, 'yyyy/mm/dd')\n"
            + "group by service_request_type");

    return sqlPecs.toString();
  }

  public String getSql5(Map cretiria) {

    StringBuilder sqlPecs = new StringBuilder("select service_request_type, count(*) REQUESTS\n"
            + "from \"M149\".\"INCIDENT\"\n"
            + "where (latitude::numeric between 41.560368 and 43.200873)\n"
            + "and (longitude::numeric -89.127758 and -86.266519)\n"
            + "group by service_request_type\n"
            + "order by REQUESTS desc\n"
            + "limit 1");

    return sqlPecs.toString();
  }

  public String getSql6(Map cretiria) {

    StringBuilder sqlPecs = new StringBuilder("select s.ssa, \n"
            + "((count(*))::numeric / (?::date - ?::date + 1))::numeric(7,2) counts\n"
            + "from \"M149\".\"INCIDENT\" i, \"M149\".\"SSA\" s \n"
            + "where i.incident_id = s.incident_id \n"
            + "and i.creation_date between to_date(?, 'yyyy/mm/dd')\n"
            + "and to_date(?, 'yyyy/mm/dd') \n"
            + "group by s.ssa order by counts desc limit 5");

    return sqlPecs.toString();
  }

  public String getSql7(Map cretiria) {

    StringBuilder sqlPecs = new StringBuilder("select q.licence_plate\n"
            + "from (\n"
            + "select licence_plate, count(*) counts \n"
            + "from \"M149\".\"VEHICLE\" \n"
            + "where licence_plate is not null\n"
            + "and licence_plate != '0'\n"
            + "and licence_plate not in ('NLP', 'UNK', 'HAS PLATES', 'NO',  'NLPS', 'DONT KNOW', 'UKNOWN', 'N/LP', 'TEMP PLATES', 'NK', 'TEMP PLATE', 'TEMP', 'UNKNOW', 'N/P', 'DOESNT KNOW PLATE', 'TEMPORARY PLATES', 'NON', 'UNKN', 'TEMPORARY', 'NOPLATES', 'CALLER DOESNT KNOW', 'NOPLATE', 'TEMPORARY PLATE', 'EXPIRED', 'REMOVED', 'DONT HAVE', 'UNSURE')\n"
            + "group by licence_plate\n"
            + "order by counts desc\n"
            + ") q where counts > 1");

    return sqlPecs.toString();
  }

  public String getSql8(Map cretiria) {

    StringBuilder sqlPecs = new StringBuilder("select color, count(*) counts \n"
            + "from \"M149\".\"VEHICLE\" \n"
            + "group by color \n"
            + "order by counts desc \n"
            + "limit 1 \n"
            + "offset 1");

    return sqlPecs.toString();
  }

  public String getSql9(Map cretiria) {
    StringBuilder sqlPecs = new StringBuilder("select *\n"
            + "from \"M149\".\"INCIDENT\" i, \"M149\".\"RODENT_BAIT\" r\n"
            + "where i.service_request_type = '6'\n"
            + "and r.premises_baited < ?\n"
            + "and i.incident_id = r.incident_id");

    return sqlPecs.toString();
  }

  public String getSql10(Map cretiria) {

    StringBuilder sqlPecs = new StringBuilder("select * \n"
            + "from \"M149\".\"INCIDENT\" i, \"M149\".\"RODENT_BAIT\" r\n"
            + "where i.service_request_type = '6'\n"
            + "and r.with_garbage < ?\n"
            + "and i.incident_id = r.incident_id");

    return sqlPecs.toString();
  }

  public String getSql11(Map cretiria) {

    StringBuilder sqlPecs = new StringBuilder("select * \n"
            + "from \"M149\".\"INCIDENT\" i, \"M149\".\"RODENT_BAIT\" r\n"
            + "where i.service_request_type = '6'\n"
            + "and r.with_rats < ?\n"
            + "and i.incident_id = r.incident_id");

    return sqlPecs.toString();
  }

  public String getSql12(Map cretiria) {
    StringBuilder sqlPecs = new StringBuilder("select distinct a.police_district from\n"
            + "(select distinct i.police_district, i.completion_date\n"
            + "from \"M149\".\"INCIDENT\" i, \"M149\".\"POT_HOLES\" ph\n"
            + "where i.service_request_type = '5'\n"
            + "and i.completion_date = '2015-01-30'\n"
            + "and i.police_district != '0'\n"
            + "and ph.filled_pot_holes > 1\n"
            + "and i.incident_id = ph.incident_id order by i.police_district) a,\n"
            + "(select distinct i.police_district, i.completion_date\n"
            + "from \"M149\".\"INCIDENT\" i, \"M149\".\"RODENT_BAIT\" rb\n"
            + "where i.service_request_type = '6'\n"
            + "and i.completion_date = '2015-01-30'\n"
            + "and i.police_district != '0'\n"
            + "and rb.premises_baited > 1\n"
            + "and i.incident_id = rb.incident_id order by i.police_district) b\n"
            + "where\n"
            + "a.police_district = b.police_district ");

    return sqlPecs.toString();
  }

  public String getSql(Map cretiria) {
    StringBuilder sqlPecs = new StringBuilder("SELECT * FROM \"M149\".\"INCIDENT\" WHERE 1=1\n");

    if (cretiria != null && cretiria.get("INCIDENT_ID") != null && !cretiria.get("INCIDENT_ID").toString().isEmpty()) {
      sqlPecs.append("AND INCIDENT_ID = ?\n");
    }
    if (cretiria != null && cretiria.get("SERVICE_REQUEST_TYPE") != null && !cretiria.get("SERVICE_REQUEST_TYPE").toString().isEmpty()) {
      sqlPecs.append("AND SERVICE_REQUEST_TYPE = ?\n");
    }
    sqlPecs.append(" ORDER BY INCIDENT_ID DESC");

    return sqlPecs.toString();
  }

  public Object[] getSqlParameters1(Map cretiria) {
    List sqlParameters = new ArrayList();

    if (cretiria.get("START_DATE") != null && !cretiria.get("START_DATE").toString().isEmpty()) {
      sqlParameters.add(cretiria.get("START_DATE").toString());
    }
    if (cretiria.get("END_DATE") != null && !cretiria.get("END_DATE").toString().isEmpty()) {
      sqlParameters.add(cretiria.get("END_DATE").toString());
    }
    return sqlParameters.toArray();
  }

  public Object[] getSqlParameters2(Map cretiria) {
    List sqlParameters = new ArrayList();

    if (cretiria.get("SERVICE_REQUEST_TYPE") != null && !cretiria.get("SERVICE_REQUEST_TYPE").toString().isEmpty()) {
      sqlParameters.add(cretiria.get("SERVICE_REQUEST_TYPE").toString());
    }
    if (cretiria.get("START_DATE") != null && !cretiria.get("START_DATE").toString().isEmpty()) {
      sqlParameters.add(cretiria.get("START_DATE").toString());
    }
    if (cretiria.get("END_DATE") != null && !cretiria.get("END_DATE").toString().isEmpty()) {
      sqlParameters.add(cretiria.get("END_DATE").toString());
    }

    return sqlParameters.toArray();
  }

  public Object[] getSqlParameters3(Map cretiria) {
    List sqlParameters = new ArrayList();

    if (cretiria.get("DATE") != null && !cretiria.get("DATE").toString().isEmpty()) {
      sqlParameters.add(cretiria.get("DATE").toString());
    }

    return sqlParameters.toArray();
  }

  public Object[] getSqlParameters4(Map cretiria) {
    List sqlParameters = new ArrayList();

    if (cretiria.get("START_DATE") != null && !cretiria.get("START_DATE").toString().isEmpty()) {
      sqlParameters.add(cretiria.get("START_DATE").toString());
    }
    if (cretiria.get("END_DATE") != null && !cretiria.get("END_DATE").toString().isEmpty()) {
      sqlParameters.add(cretiria.get("END_DATE").toString());
    }

    return sqlParameters.toArray();
  }

  public Object[] getSqlParameters5(Map cretiria) {
    List sqlParameters = new ArrayList();
    /*
    if (cretiria.get("LAT1") != null && !cretiria.get("LAT1").toString().isEmpty()) {
      sqlParameters.add(cretiria.get("LAT1").toString());
    }
    if (cretiria.get("LONG1") != null && !cretiria.get("LONG1").toString().isEmpty()) {
      sqlParameters.add(cretiria.get("LONG1").toString());
    }
    if (cretiria.get("LAT2") != null && !cretiria.get("LAT2").toString().isEmpty()) {
      sqlParameters.add(cretiria.get("LAT2").toString());
    }
    if (cretiria.get("LONG2") != null && !cretiria.get("LONG2").toString().isEmpty()) {
      sqlParameters.add(cretiria.get("LONG2").toString());
    }
     */

    return sqlParameters.toArray();
  }

  public Object[] getSqlParameters6(Map cretiria) {
    List sqlParameters = new ArrayList();

    return sqlParameters.toArray();
  }

  public Object[] getSqlParameters7(Map cretiria) {
    List sqlParameters = new ArrayList();

    return sqlParameters.toArray();
  }

  public Object[] getSqlParameters8(Map cretiria) {
    List sqlParameters = new ArrayList();

    return sqlParameters.toArray();
  }

  public Object[] getSqlParameters9(Map cretiria) {
    List sqlParameters = new ArrayList();

    return sqlParameters.toArray();
  }

  public Object[] getSqlParameters10(Map cretiria) {
    List sqlParameters = new ArrayList();

    return sqlParameters.toArray();
  }

  public Object[] getSqlParameters11(Map cretiria) {
    List sqlParameters = new ArrayList();

    return sqlParameters.toArray();
  }

  public Object[] getSqlParameters12(Map cretiria) {
    List sqlParameters = new ArrayList();

    return sqlParameters.toArray();
  }

  public Object[] getSqlParameters(Map cretiria) {
    List sqlParameters = new ArrayList();

    if (cretiria.get("INCIDENT_ID") != null && !cretiria.get("INCIDENT_ID").toString().isEmpty()) {
      sqlParameters.add(cretiria.get("INCIDENT_ID").toString());
    }
    if (cretiria.get("SERVICE_REQUEST_TYPE") != null && !cretiria.get("SERVICE_REQUEST_TYPE").toString().isEmpty()) {
      sqlParameters.add(cretiria.get("SERVICE_REQUEST_TYPE").toString());
    }
    return sqlParameters.toArray();
  }

}
