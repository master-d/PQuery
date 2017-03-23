package dbutil;

public enum DBL {
	ACCELA("jdbc/accela"),
	ADMINUSER("jdbc/adminuser"),
	BICLIC("jdbc/biclic"),
	CAD("jdbc/cad"),
	CAMPAIGN_FINANCE("jdbc/campaign_finance"),
	CHILI("jdbc/ChiliNet"),
	CITY_PROJ("jdbc/cityproj"),
	CSR("jdbc/csr_test","jdbc/csr"),
	DATA_INV("jdbc/datainv"),
	DPW("jdbc/dpwtest","jdbc/dpwprod",DBInstance.MINTANDITMD),
	DPW_CSR_LAGAN("jdbc/csr_test","jdbc/dpwlagan_write"), //This should never be changed it is how XY_Eserv changes between test and production inserts
	DPW_LAGAN("jdbc/dpwlagan"), //This should never be changed it is how XY_Eserv always gets production when needed
	ENOTIFY("jdbc/enotifytest","jdbc/enotify5",DBInstance.MINTANDITMD),
	EUSER("jdbc/euserTest","jdbc/euser",DBInstance.MINTANDITMD),
	EVAULT("jdbc/evault"),
	FWC("jdbc/fwctest","jdbc/fwc"),
	GIS("jdbc/oragisv5"),
	LAGAN("jdbc/laganTest","jdbc/laganProd",DBInstance.ITMDAPPS),
	LIRA("jdbc/liratest","jdbc/liraprod",DBInstance.ITMDAPPS),
	LMS("jdbc/lms"),
	LUZMGR("jdbc/luzmgr"),
	MFDUSER("jdbc/mfduser"),
	MPDACCIDENTS("jdbc/mpdAccidents"),
	MPD_CRIME("jdbc/mpdcrimetest","jdbc/mpdcrime",DBInstance.MINTANDITMD),
	MY_MILW_HOME("jdbc/mymilwhome"),
	NSHR("jdbc/nshr"),
	PITS("jdbc/pits"),
	PROPREG("jdbc/PropRegTest","jdbc/PropReg",DBInstance.MINTANDITMD),
	QUIZ("jdbc/Quiz"),
	RMS("jdbc/rms"),
	TAXBILL("jdbc/taxbill"),
	WEB_APP_LOGS("jdbc/webapplogs"),
	WORK_SITE_SURVEY("jdbc/workSiteSurvey"),
	NSS("jdbc/nss"),
	LICWORKFLOW("jdbc/licworkflow"),
	WEBGEOSADM("jdbc/webgeosadmtest","jdbc/webgeosadm",DBInstance.MINTANDITMD),
	MKETRACKER("jdbc/MkeTracker"),
	REVLOG("jdbc/revlog_user");

	DBInstance instance;
	private DBL(String jndi1){
		instance = new DBInstance(jndi1);
	}
	private DBL(String jndi1, String jndi2){
		instance = new DBInstance(jndi1, jndi2);
	}
	private DBL(String jndi1, String jndi2, int dbinstance){
		instance = new DBInstance(jndi1, jndi2, dbinstance);
	}
	public DBInstance getInstance(){
		return instance;
	}
	public void setProdLocation(int prodLocation){
		instance.setProdLocation(prodLocation);
	}
	public static final void forceProd(){
		for(DBL dbi : DBL.values()){
			dbi.setProdLocation(DBInstance.ALL);
		}
	}
	public static final void forceProd(DBL... schema_idxs){
		for(DBL dbi : schema_idxs){
			dbi.setProdLocation(DBInstance.ALL);
		}
	}
	public static final void forceTest(){
		for(DBL dbi : DBL.values()){
			dbi.setProdLocation(DBInstance.NONE);
		}
	}
	public static final void forceTest(DBL... schema_idxs){
		for(DBL dbi : schema_idxs){
			dbi.setProdLocation(DBInstance.NONE);
		}
	}
	public static String getAllJNDI(){
		String x = "";
		for(DBL dbi : DBL.values()){
			x += dbi.name() + ":" + dbi.getInstance().getJndi() + " ||";
		}
		return x;
	}
}