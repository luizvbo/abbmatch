from abbmatch.wiki_db import WikiDB
from wikipedia import WikipediaPage, PageError
from abbmatch import logger
from multiprocessing import Pool, cpu_count


class WikiScraper:
    def __init__(self, starting_link):
        self.wiki_db = WikiDB()
        self.wiki_db.create_database()
        self.wiki_db.insert_one('link_map', values=(0, 0, -1, starting_link))

    def get_unvisited_links(self, max_depth=6, limit=30):
        sql = F"""
            SELECT id, link, depth FROM link_map as A
            LEFT JOIN content as B ON A.id=B.id_link
            WHERE B.content is NULL AND depth <= {max_depth}
            ORDER BY depth, id LIMIT {limit}
        """
        return self.wiki_db._fetchall(sql)

    def scrape_wiki(self):
        unvisited = self.get_unvisited_links()
        cpu_count_ = cpu_count()
        while len(unvisited) > 0:
            with Pool(processes=cpu_count_) as pool:
                # Each item from unvisited is composed by id, link, depth
                res = pool.map(process_page, unvisited)
            # Writing the results
            for id_link, summary, depth, title, links in res:
                if id_link is None:
                    logger.info(F"Page '{title}' (id {links}) does not match any pages. Try another id!")
                    self.wiki_db.insert_one('content', ('id_link', 'content', 'title'),
                                            (id_link, None, None))
                else:
                    logger.info("Storing content for: " + title)
                    self.wiki_db.insert_one('content', ('id_link', 'content', 'title'),
                                            (id_link, summary, title))
                    self.store_links(id_link, depth, links)

            # for id_link, link in unvisited:
            #     logger.info(F"Processing link: '{link}'")
            #     self.process_page(id_link, link)
            unvisited = self.get_unvisited_links()

    def store_links(self, parent_id, depth, link_list):
        # self.wiki_db._insert_many()

        # sql = F"INSERT INTO link_map (parent_id, link) VALUES ({parent_id}, ?)"
        link_list = [(l,) for l in link_list]
        self.wiki_db.insert_many(table='link_map', col=('parent_id', 'depth', 'link'),
                                 value_placeholder=(parent_id, depth, None),
                                 values=link_list)
        # _insert_many(sql, link_list)


def process_page(params):
    id_link, link, depth = params
    # Get the wiki object for the link
    try:
        wiki_obj = WikipediaPage(link)
    except PageError:
        return (None, None, None, link, id_link)
    title = wiki_obj.title
    summary = wiki_obj.summary[:300].replace("\"", "'")
    links = wiki_obj.links
    return (id_link, summary, depth+1, title, links)
