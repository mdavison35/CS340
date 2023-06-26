from pymongo import MongoClient
from bson.objectid import ObjectId
import copy

class AnimalShelter(object):
	""" CRUD operations for Animal collection in MongoDB """
	
	def __init__(self, USER, PASS):
		""" Class constructor.
		Logs into MongoDB using specified username and password. Database, collection and
		port are hard-coded.
		
		Keyword arguments:
		self -- AnimalShelter, the class instance
		USER -- string, the username used to log into mongoDB
		PASS -- string, the password used to log into MongoDB
		"""
		DB = 'AAC'
		COL = 'animals'
		
		mongo_address = 'mongodb://' + USER + ':' + PASS + '@nv-desktop-services.apporto.com:30954'
		
		self.client = MongoClient('mongodb://aacuser:r682w394@nv-desktop-services.apporto.com:30954')
		self.database = self.client['%s' % (DB)]
		self.collection = self.database['%s' % (COL)]
		
	def create(self, data):
		""" Creates a new entry in the mongoDB collection with the data parameter key
		value pair(s).
		
		Keyword arguments:
		self -- AnimalShelter, the class instance
		data -- Dictionary, object to add to mondoDB
		"""
		if data is not None:
			# add object to the collection
			self.database.animals.insert_one(data)
			return True
		else:
			raise Exception("Nothing to save, because data parameter is empty")
			return False
			
	def read(self, data):
		""" Finds and returns details of object in mongoDB with data parameter key
		value pair(s).
		
		Keyword arguments:
		self -- AnimalShelter, the class instance
		data -- Dictionary, object to find and return details from mongoDB
		"""
		#if data is not None:
		
		animals = self.database.animals.find(data)
		text_output = ""
		
		for animal in animals:
			text_output = text_output + str(animal) + "\n\n"
		# return the output from MongoDB for the objects
		return text_output
		#else:
			#raise Exception("Cannot find entry, because data parameter is empty")
			
	def get_objects(self, data):
		animals = self.database.animals.find(data)
		return animals
			
	def update(self, data, updateData):
		""" Updates entry in the mongoDB collection with the data parameter key
		value pair(s) to have updateData key value pair(s).
		
		Keyword arguments:
		self -- AnimalShelter, the class instance
		data -- Dictionary, object to update in mondoDB
		updateData -- Dictionary, key value pairs to update for the object
		"""
		if data is not None:
			#store the mongoDBcursor for data key value pair in a variable
			animals = self.database.animals.find(data)
			
			if (animals.count() > 1):
				# updates all entries with the specified key value pair
				self.database.animals.update_many(data, {"$set": updateData})
				return animals.count()
				
			elif (animals.count() == 1):
				# updates the entry with the specified key value pair
				self.database.animals.update_one(data, {"$set": updateData})
				return 1
			else:
				return 0
		else:
			raise Exception("Cannot update entry, because data parameter is empty")
			
	def delete(self, data):
		""" Deletes entry in the mongoDB collection with the data parameter key
		value pair(s).
		
		Keyword arguments:
		self -- AnimalShelter, the class instance
		data -- Dictionary, object to delete in mondoDB
		"""
		if data is not None:
			#store the mongoDB cursor for data key value pair in a variable
			animals = self.database.animals.find(data)
			
			#store the number of entries found in mongodb, so it doesn't change 
			#after records deleted
			animals_count = animals.count()
			
			if (animals.count() > 1):
				# deletes all entries with the specified key value pair
				self.database.animals.delete_many(data)
				return animals_count
				
			elif (animals.count() == 1):
				# deletes the entry with the specified key value pair
				self.database.animals.delete_one(data)
				return 1
			else:
				return 0
		else:
			raise Exception("Cannot delete entry, because data parameter is empty")
			
	def __read_preferred_dogs__(self, preferred_breeds, preferred_sex, training_ages):
		breed_query = {'breed': {'$in': preferred_breeds}}
		sex_query = {'sex_upon_outcome': preferred_sex}
		age_query = {'age_upon_outcome_in_weeks': {'$gt': training_ages[0], '$lt': training_ages[1]}}
		
		animals = self.database.animals.find({'$and': [breed_query, sex_query, age_query]})
		
		return animals
			
	def read_water_dogs(self):
		preferred_breeds = ['Labrador Retriever Mix', 'Chesapeake Bay Retriever', 'Newfoundland']
		preferred_sex = 'Intact Female'
		training_ages = [26, 156]
		
		water_dogs = self.__read_preferred_dogs__(preferred_breeds, preferred_sex, training_ages)
		
		return water_dogs
	
	def read_mountain_dogs(self):
		preferred_breeds = ['German Shepherd', 'Alaskan Malamute', 'Old English Sheepdog', 'Siberian Husky',
					'Rottweiler']
		preferred_sex = 'Intact Male'
		training_ages = [26, 156]
		
		mountain_dogs = self.__read_preferred_dogs__(preferred_breeds, preferred_sex, training_ages)
		
		return mountain_dogs
		
	def read_disaster_dogs(self):
		preferred_breeds = ['Doberman Pinscher', 'German Shepherd', 'Golden Retriever', 'Bloodhound', 'Rottweiler']
		preferred_sex = 'Intact Male'
		training_ages = [20, 300]
		
		disaster_dogs = self.__read_preferred_dogs__(preferred_breeds, preferred_sex, training_ages)
		
		return disaster_dogs

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
