local Promise = {}      
Promise.__index = Promise   
Promise.__tostring = function(self) return `Promise <{self.State}>: {self.__address:sub(8)}` end                               

export type Promise = { 	  
	State: "pending" | "fulfilled" | "rejected" | "cancelled" | "unknown", 	

	resolve: (
		self: Promise,
		...any?
	) -> (), 	
	reject: (
		self: Promise,
		reason: string,
		...any?
	) -> (), 	

	next: (
		self: Promise,
		onFulfilled: (...any) -> ...any,
		onRejected: (reason: string, ...any) -> ...any
	) -> Promise, 	 	

	await: (
		self: Promise,
		suppressError: boolean?
	) -> (boolean, ...any) | any, 
	cancel: (self: Promise) -> (), 	
} 	 	

function Promise.isPromise(p: Promise?): boolean 	
	return type(p) == "table" and getmetatable(p) == Promise 	
end 	

function Promise.new(executor) 
	local self = {
		State = "pending",  
		_handlers = {},
		_value = {},    
		_running = nil, 	
	}
	self.__address = tostring(self) 	
	setmetatable(self, Promise) 	
	if executor then 	
		self._running = task.defer(function()
			local ok, response = pcall(
				executor,
				function(...) self:resolve(...) end,
				function(...) self:reject(...) end)   
			if not ok then
				self:reject(response)   
			end     
		end)    
	end     
	return self 
end 

function Promise:_getValues(): ...any return unpack(self._value) end

function Promise:_executeHandler(handler): ()   
	local callback = handler[self.State]     	 	 	
	if callback then
		local ok, response = pcall(callback, self:_getValues()) 	 	 		  	 		 	
		if ok then
			if Promise.isPromise(response) then 	
				response:next( 	
					function(...) handler.child:resolve(...) end,
					function(...) handler.child:reject(...) end) 		 	
			else
				handler.child:resolve(response) 	
			end 		 
		else    
			handler.child:reject(response)  
		end 
	else
		if self.State == "fulfilled" then
			handler.child:resolve(self:_getValues()) 
		else
			handler.child:reject(self:_getValues()) 
		end     
	end     
end     

function Promise:resolve(...: any?): ()   
	if self.State == "pending" then
		self.State = "fulfilled"    
		self._value = {...}                  
		for _, h in self._handlers do self:_executeHandler(h) end     
	end     
end     

function Promise:reject(...: any?): ()   
	if self.State == "pending" then
		self.State = "rejected"         
		self._value = {...}   
		for _, h in self._handlers do self:_executeHandler(h) end    
	end     
end       

function Promise:cancel(): ()  	
	if self.State == "pending" then 
		for _, h in self._handlers do h.child:cancel() end     	
		if self._running and coroutine.status(self._running) == "running" then 
			coroutine.close(self._running)	 	  
		end   
		self.State = "cancelled"
	end
end

function Promise:await(suppress) 	
	local handle = function()
		if self.State == "fulfilled" then  		 
			if suppress then
				return true, self:_getValues()
			else
				return self:_getValues()
			end 	 
		elseif self.State == "rejected" then
			if suppress then  	 		 	 	  	 	 	   	 	 
				return false, self:_getValues()
			else 	 
				return error(`Promise yielded error{self._value[1] and ` "{self._value[1]}"` or ""}`)
			end 	   	 
		end 
	end 		 
	if self.State == "pending" then
		local thread = coroutine.running()  		
		self:next(
			function(...) task.spawn(thread) end,--, true, ...) end,
			function(...) task.spawn(thread) end)-- , false, ...) end) 	
		coroutine.yield()  	  	 	 	
		return handle()  	  	  		  
	else  	    
		return handle()
	end  
end

function Promise:next(onFulfilled, onRejected): ()         
	local new = Promise.new()         
	local handler = {
		child = new,
		fulfilled = onFulfilled,
		rejected = onRejected, }  	 	
	if self.State == "pending" then     
		table.insert(self._handlers, handler)         
	else
		task.defer(self._executeHandler, self, handler)      
	end      				
	return new  
end      		

return (Promise :: {
	new: (
		executor: (    
			resolve: (...any?) -> (),
			reject: (reason: string, ...any?
			) -> ()   	     
		) -> ()) -> Promise, 		
	isPromise: (Promise?) -> boolean, 	
})

 
