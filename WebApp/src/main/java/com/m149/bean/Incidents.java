package com.m149.bean;

import java.io.Serializable;

import javax.inject.Named;
import javax.faces.view.ViewScoped;

/**
 *
 * @author stavropoulosp
 */
@Named("bean")
@ViewScoped
public class Incidents implements Serializable {

   public String getMessage() {
      return "Hello World from Fuertefentura";
   }

}
