package com.example.tara;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class TaraWebErrorHandler implements ExceptionMapper<Throwable> {

    @Override
    public Response toResponse(Throwable exception) {
        exception.printStackTrace();
        return Response.serverError().build();
    }
}

