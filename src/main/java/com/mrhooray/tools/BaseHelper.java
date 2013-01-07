package com.mrhooray.tools;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import twitter4j.Status;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public abstract class BaseHelper {
	protected static String toJson(Status status) {
		Gson gson = new Gson();
		JsonObject json = (JsonObject) gson.toJsonTree(status);
		json.remove("createdAt");
		json.addProperty("createdAt", getUTC(status.getCreatedAt()));
		return json.toString();
	}

	protected static String getUTC(Date date) {
		String ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss zzz";
		SimpleDateFormat sdf = new SimpleDateFormat(ISO_FORMAT);
		TimeZone utc = TimeZone.getTimeZone("UTC");
		sdf.setTimeZone(utc);
		return sdf.format(date);
	}
}
