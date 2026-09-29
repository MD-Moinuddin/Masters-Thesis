package com.example.tara;

public class TaraLog {


	private static final int DEBUG = resolveLevel();

	private static int resolveLevel() {
		Integer prop = Integer.getInteger("tara.log");
		if (prop != null) return prop;
		String env = System.getenv("TARA_LOG");
		if (env != null) {
			try { return Integer.parseInt(env.trim()); } catch (NumberFormatException ignored) {}
		}
		return 1;
	}

	public static void error(Object c, String s) {
		synchronized (TaraLog.class) {
			System.out.println("[" + System.currentTimeMillis() + "|ERRO] " + c + " " + s);
		}
	}

	@SuppressWarnings("unused")
	public static void warning(Object c, String s) {
		if (DEBUG > 0) {
			synchronized (TaraLog.class) {
				System.out.println("[" + System.currentTimeMillis() + "|WARN] " + c + " " + s);
			}
		}
	}
	
	@SuppressWarnings("unused")
	public static void info(Object c, String s) {
		if (DEBUG > 1) {
			synchronized (TaraLog.class) {
				System.out.println("[" + System.currentTimeMillis() + "|INFO] " + c + " " + s);
			}
		}
	}
	
	@SuppressWarnings("unused")
	public static void debug(Object c, String s) {
		if (DEBUG > 2) {
			synchronized (TaraLog.class) {
				System.out.println("[" + System.currentTimeMillis() + "|DEBG] " + c + " " + s);
			}
		}
	}

	public static void plain(String s) {
		synchronized (TaraLog.class) {
			System.out.println(s);
		}
	}
	
}
