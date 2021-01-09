package com.m149.bean;

import com.m149.persistence.IncidentsPersistence;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.PostConstruct;
import javax.inject.Named;
import javax.faces.view.ViewScoped;
import org.primefaces.model.LazyDataModel;
import org.primefaces.model.SortOrder;

/**
 *
 * @author stavropoulosp
 */
@Named("bean")
@ViewScoped
public class Incidents implements Serializable {

  IncidentsPersistence persistence = new IncidentsPersistence();

  private int queryNumber = 0;

  private static final Logger logger = Logger.getLogger(Incidents.class.getName());

  private LazyDataModel<Map> lazyDataModelIncidents = null;

  Map criteria = new HashMap();

  @PostConstruct
  public void init() {
    lazyDataModelIncidents = null;
    criteria = new HashMap();
    queryNumber = 0;
  }

  public void searchView() {
    lazyDataModelIncidents = null;
  }

  public void searchClear() {
    lazyDataModelIncidents = null;
    criteria = new HashMap();
  }

  public LazyDataModel<Map> getLazyDataModelPecs() {
    if (lazyDataModelIncidents == null & !criteria.isEmpty()) {
      lazyDataModelIncidents = new LazyDataModel<Map>() {
        public void setRowIndex(int rowIndex) {
          if (rowIndex == -1 || getPageSize() == 0) {
            super.setRowIndex(-1);
          } else {
            super.setRowIndex(rowIndex % getPageSize());
          }
        }
        private int rowsCount = -1;

        public int getRowCount() {
          try {
            if (rowsCount < 0) {
              rowsCount = persistence.selectCount(criteria, queryNumber);
              this.setRowCount(rowsCount);
            }
          } catch (Exception ex) {
            logger.log(Level.SEVERE, ex.getLocalizedMessage(), ex);
            rowsCount = 0;
            this.setRowCount(rowsCount);
          }
          return rowsCount;
        }

        @Override
        public List<Map> load(int first, int pageSize, String sortField, SortOrder sortOrder, Map<String, Object> filters) {
          try {
            int page = first >= getRowCount() ? (getRowCount() - ((getRowCount() - 1) % pageSize + 1)) : first;
            List<Map> listTable = persistence.lazySelect(criteria, page, pageSize, queryNumber);
            return listTable;
          } catch (Exception ex) {
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, ex.getLocalizedMessage(), ex);
            return null;
          }
        }
      };
    }
    return lazyDataModelIncidents;
  }

  public Map getCriteria() {
    return criteria;
  }

  public void setCriteria(Map criteria) {
    this.criteria = criteria;
  }

  public int getQueryNumber() {
    return queryNumber;
  }

  public void setQueryNumber(int queryNumber) {
    this.queryNumber = queryNumber;
    criteria = new HashMap();
    lazyDataModelIncidents = null;
  }

}
