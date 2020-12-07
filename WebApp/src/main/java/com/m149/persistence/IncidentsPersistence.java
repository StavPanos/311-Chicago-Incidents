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

    switch (queryNumber) {
      case (1):
        rowsCount = jdbcFetch.selectCount(getSql1(criteria), getSqlParameters1(criteria));
        break;
      case (9):
        rowsCount = jdbcFetch.selectCount(getSql9(criteria), getSqlParameters9(criteria));
        break;
      case (3):
        rowsCount = jdbcFetch.selectCount(getSql3(criteria), getSqlParameters3(criteria));
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

    switch (queryNumber) {
      case (1):
        listTable = jdbcFetch.select(getSql1(criteria), getSqlParameters1(criteria), page, pageSize);
        break;
      case (9):
        listTable = jdbcFetch.select(getSql9(criteria), getSqlParameters9(criteria), page, pageSize);
        break;
      case (3):
        listTable = jdbcFetch.select(getSql3(criteria), getSqlParameters3(criteria), page, pageSize);
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

  public String getSql9(Map cretiria) {
    StringBuilder sqlPecs = new StringBuilder("select * \n"
            + "from \"M149\".\"INCIDENT\" i, \"M149\".\"RODENT_BAIT\" r \n"
            + "where i.service_request_type = '6'\n"
            + "and r.premises_baited < 7\n"
            + "and i.incident_id = r.incident_id");

    return sqlPecs.toString();
  }

  public String getSql3(Map cretiria) {
    StringBuilder sqlPecs = new StringBuilder("select r.description, i.zip_code, count(*) requests from \"M149\".\"INCIDENT\" i, \"M149\".\"REQUEST_TYPE\" r\n"
            + "where i.creation_date = to_date('2011-03-29', 'yyyy/mm/dd')\n"
            + " and i.zip_code != '0'\n"
            + "group by r.description, i.zip_code\n"
            + "order by r.description, i.zip_code");

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

  public Object[] getSqlParameters3(Map cretiria) {
    List sqlParameters = new ArrayList();

    if (cretiria.get("DATE") != null && !cretiria.get("DATE").toString().isEmpty()) {
      //sqlParameters.add(cretiria.get("DATE").toString());
    }

    return sqlParameters.toArray();
  }

  public Object[] getSqlParameters9(Map cretiria) {
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
