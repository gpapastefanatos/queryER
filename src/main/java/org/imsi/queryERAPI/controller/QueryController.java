package org.imsi.queryERAPI.controller;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.rowset.CachedRowSetImpl;

import org.apache.catalina.mapper.Mapper;
import org.imsi.queryERAPI.util.PagedResult;
import org.imsi.queryERAPI.util.ResultSetToJsonMapper;
import org.imsi.queryEREngine.imsi.er.QueryEngine;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

import static org.springframework.http.ResponseEntity.ok;

@RestController()
@RequestMapping("/api")
@CrossOrigin
public class QueryController {

	ResultSet rs;
	CachedRowSet rowset;
	List<ObjectNode> results = null;
	String query = "";
	@PostMapping("/query")
	public ResponseEntity<String> query(@RequestParam(value = "q", required = true) String q,
			@RequestParam(value = "page", required = true) int page, 
			@RequestParam(value = "offset", required = true) int offset) throws SQLException, JsonProcessingException {
		PagedResult pagedResult = null;
		ObjectMapper mapper = new ObjectMapper();

		page +=1;
		QueryEngine qe = new QueryEngine();
		if(!this.query.contentEquals(q)) {
			rs = qe.runQuery(q);		
			//results = ResultSetToJsonMapper.mapResultSet(rs);
			RowSetFactory factory = RowSetProvider.newFactory();
			rowset = factory.createCachedRowSet();			 
			rowset.populate(rs);
			this.query = q;
		}
//		if(offset == -1)  return ok(mapper.writeValueAsString(new PagedResult(1, results, results.size())));
//
//		int pages = (int) Math.floor(results.size() / offset) + 1;
//		
//		int resultOffset = offset * page;
//		int startOffset = resultOffset - offset;
//		if(page == pages) {
//			startOffset = offset * (page - 1);
//			resultOffset = results.size();
//			
//		}
//		if(resultOffset >= offset)
//			pagedResult = new PagedResult(pages, results.subList(startOffset, resultOffset), results.size());
//		else
//			pagedResult = new PagedResult(pages, results, results.size());
//		
//		return ok(mapper.writeValueAsString(pagedResult));
//		
		int end = rowset.size();
		int pages = (int) Math.floor(end / offset) + 1;
		
		int resultOffset = offset * page;
		int startOffset = resultOffset - offset;
		if(page == pages) {
			startOffset = offset * (page - 1);
			resultOffset = end;
			
		}
		if(resultOffset < offset || offset == -1) {
			startOffset = 1;
			resultOffset = end;
		}
		if(startOffset == 0) startOffset = 1;
		results = ResultSetToJsonMapper.mapCRS(rowset, startOffset, resultOffset);

		return ok(mapper.writeValueAsString(new PagedResult(pages, results, end)));



	}

}
