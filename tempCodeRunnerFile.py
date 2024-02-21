def find_all_people():
    people = person_collection.find()

    for person in people:
        printer.pprint(person)
        
find_all_people()