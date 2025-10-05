import requests
from bs4 import BeautifulSoup
import pandas as pd


def scrape_quotes(base_url="http://quotes.toscrape.com"):
    """
    دالة لجلب جميع الاقتباسات من موقع quotes.toscrape.com
    وترجع قائمة من القواميس تحتوي (quote, author, tags, source)
    """
    all_quotes = []
    page_url = "/page/1/"

    while page_url:
        response = requests.get(base_url + page_url)
        soup = BeautifulSoup(response.text, "html.parser")

        # كل اقتباس داخل div class="quote"
        quotes_blocks = soup.find_all("div", class_="quote")

        for block in quotes_blocks:
            quote_text = block.find("span", class_="text").get_text()
            author = block.find("small", class_="author").get_text()

            # استخراج الوسوم (tags)
            tags = [tag.get_text() for tag in block.find_all("a", class_="tag")]
            tags_joined = ", ".join(tags) if tags else "No tags"

            all_quotes.append({
                "quote": quote_text,
                "author": author,
                "tags": tags_joined,
                "source": base_url
            })

        # الانتقال للصفحة التالية إن وُجدت
        next_button = soup.find("li", class_="next")
        page_url = next_button.a["href"] if next_button else None

    return all_quotes


def main():
    quotes = scrape_quotes()
    df = pd.DataFrame(quotes)
    df.to_csv("all_quotes_with_tags.csv", index=False, encoding="utf-8")
    print(f"✅ تم حفظ {len(quotes)} اقتباس في all_quotes_with_tags.csv")


if __name__ == "__main__":
    main()
