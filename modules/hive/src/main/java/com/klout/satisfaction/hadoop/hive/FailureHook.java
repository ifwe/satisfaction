package com.klout.satisfaction.hadoop.hive;

import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;

public class FailureHook implements ExecuteWithHookContext {

	@Override
	public void run(HookContext ctxt) throws Exception {

       System.out.println(" FAILURE !!!! " + ctxt.getOperationName());
    

       System.out.println("QP=" +  ctxt.getQueryPlan()  );
       System.out.println( ctxt.getQueryPlan().getQueryString());

	}

}
