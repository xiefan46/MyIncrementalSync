package com.alibaba.middleware.race.sync.model;

import com.alibaba.middleware.race.sync.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by xiefan on 6/7/17.
 */
public class Statistics {

	private static final Logger	logger		= LoggerFactory.getLogger(Server.class);

	private long				startId;

	private long				endId;

	private Set<Long>			pkUpdateSet	= new HashSet<>();

	private int				count;

	private int				uCount;

	private int				dCount;

	private int				iCount;

	private int				uCount1Filter;

	private int				dCount1Filter;

	private int				iCount1Filter;

	private int				uCountIn;

	private int				uCountOut;

	private int				pkUpdateCount;

	public Statistics(long startId, long endId) {
		this.startId = startId;
		this.endId = endId;
	}

	public void dealRecord(RecordLog record) {
		count++;
		switch (record.getAlterType()) {
		case Constants.INSERT:
			iCount++;
			if (in((long) record.getPrimaryColumn().getLongValue()))
				iCount1Filter++;
			break;
		case Constants.UPDATE:
			uCount++;
			if (in((long) record.getPrimaryColumn().getLongValue()))
				uCount1Filter++;
			if (record.isPKUpdate()) {
				pkUpdateCount++;
				long oldPk = (long) record.getPrimaryColumn().getBeforeValue();
				long newPk = (long) record.getPrimaryColumn().getLongValue();
				pkUpdateSet.add(oldPk);
				pkUpdateSet.add(newPk);
				if (in(newPk))
					uCountIn++;
				if (in(oldPk))
					uCountOut++;
			}
			break;

		case Constants.DELETE:
			dCount++;
			if (in((long) record.getPrimaryColumn().getLongValue()))
				dCount1Filter++;
			break;
		}
	}

	public static Statistics combine(Statistics stat1, Statistics stat2) {
		Statistics stat = new Statistics(stat1.getStartId(), stat1.getEndId());
		stat.setCount(stat1.getCount() + stat2.getCount());
		stat.setiCount(stat1.getiCount() + stat2.getiCount());
		stat.setuCount(stat1.getuCount() + stat2.getuCount());
		stat.setdCount(stat1.getdCount() + stat2.getdCount());
		stat.setiCount1Filter(stat1.getiCount1Filter() + stat2.getiCount1Filter());
		stat.setuCount1Filter(stat1.getuCount1Filter() + stat2.getuCount1Filter());
		stat.setdCount1Filter(stat1.getdCount1Filter() + stat2.getdCount1Filter());
		stat.setuCountIn(stat1.getuCountIn() + stat2.getuCountIn());
		stat.setuCountOut(stat1.getuCountOut() + stat2.getuCountOut());
		stat.setPkUpdateCount(stat.getPkUpdateCount() + stat2.getPkUpdateCount());
		stat.getPkUpdateSet().addAll(stat1.getPkUpdateSet());
		stat.getPkUpdateSet().addAll(stat2.getPkUpdateSet());
		return stat;
	}

	public void printStat() {
		logger.info("Print stat. Thread id : {}. StartId : {}. EndId : {}",
				Thread.currentThread().getId(), startId, endId);
		logger.info("record count : {}. iCount : {} . uCount : {} . dCount : {} ", count, iCount,
				uCount, dCount);
		logger.info("uCountFilter : {} , iCountFilter : {} , dCountFilter : {}", uCount1Filter,
				iCount1Filter, dCount1Filter);
		logger.info("Pk update set size : {} , Pk update count : {}", pkUpdateSet.size(),
				pkUpdateCount);
		logger.info("uCountIn : {}, uCountOut : {}", uCountIn, uCountOut);
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public int getuCount() {
		return uCount;
	}

	public void setuCount(int uCount) {
		this.uCount = uCount;
	}

	public int getdCount() {
		return dCount;
	}

	public void setdCount(int dCount) {
		this.dCount = dCount;
	}

	public int getiCount() {
		return iCount;
	}

	public void setiCount(int iCount) {
		this.iCount = iCount;
	}

	public int getuCount1Filter() {
		return uCount1Filter;
	}

	public void setuCount1Filter(int uCount1Filter) {
		this.uCount1Filter = uCount1Filter;
	}

	public int getdCount1Filter() {
		return dCount1Filter;
	}

	public void setdCount1Filter(int dCount1Filter) {
		this.dCount1Filter = dCount1Filter;
	}

	public int getiCount1Filter() {
		return iCount1Filter;
	}

	public void setiCount1Filter(int iCount1Filter) {
		this.iCount1Filter = iCount1Filter;
	}

	public int getuCountIn() {
		return uCountIn;
	}

	public void setuCountIn(int uCountIn) {
		this.uCountIn = uCountIn;
	}

	public int getuCountOut() {
		return uCountOut;
	}

	public void setuCountOut(int uCountOut) {
		this.uCountOut = uCountOut;
	}

	private boolean in(long value) {
		return value > startId && value < endId;
	}

	public long getStartId() {
		return startId;
	}

	public void setStartId(long startId) {
		this.startId = startId;
	}

	public long getEndId() {
		return endId;
	}

	public void setEndId(long endId) {
		this.endId = endId;
	}

	public Set<Long> getPkUpdateSet() {
		return pkUpdateSet;
	}

	public int getPkUpdateCount() {
		return pkUpdateCount;
	}

	public void setPkUpdateCount(int pkUpdateCount) {
		this.pkUpdateCount = pkUpdateCount;
	}

	public void setPkUpdateSet(Set<Long> pkUpdateSet) {
		this.pkUpdateSet = pkUpdateSet;
	}

}
