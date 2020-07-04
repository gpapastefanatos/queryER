package org.imis.er.DataStructures;

import java.io.Serializable;


public class Comparison implements Serializable {

	private static final long serialVersionUID = 723425435776147L;

	private final boolean cleanCleanER;
	private final int entityId1;
	private final int entityId2;
	private double utilityMeasure;
	private boolean hasQuery;

	public Comparison (boolean ccER, int id1, int id2) {
		cleanCleanER = ccER;
		entityId1 = id1;
		entityId2 = id2;
		utilityMeasure = -1;
	}

	

	public int getEntityId1() {
		return entityId1;
	}

	public int getEntityId2() {
		return entityId2;
	}

	public double getUtilityMeasure() {
		return utilityMeasure;
	}

	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + entityId1;
		result = prime * result + entityId2;
		return result;
	}



	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Comparison other = (Comparison) obj;
		if (entityId1 != other.entityId1)
			return false;
		if (entityId2 != other.entityId2)
			return false;
		return true;
	}



	public boolean isCleanCleanER() {
		return cleanCleanER;
	}

	public void setUtilityMeasure(double utilityMeasure) {
		this.utilityMeasure = utilityMeasure;
	}



	public boolean hasQuery() {
		return this.hasQuery;
	}



	public void setHasQuery(boolean hasQuery) {
		this.hasQuery = hasQuery;
	}
}