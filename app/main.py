import os
import random

from flask import Flask, \
                  request, redirect, render_template, url_for
app = Flask(__name__)

image_dir = os.path.join('static', 'css', 'images')

@app.route('/')
def index():
    images = random.sample(os.listdir(image_dir), 20)
    im_meta = [dict([('title', 'title-text'), 
                     ('src', os.path.join(image_dir, images[i]))]) \
               for i in range(20)]
    return render_template('index.html', im_meta=im_meta)

@app.route('/recommend', methods=['GET', 'POST'])
def onclick_recommend():
    if request.method == 'POST':
        books = request.form.getlist('book')

        # call our recommendation function and return values to display

        images = random.sample(os.listdir(image_dir), 5)
        im_meta = [dict([('title', 'title-text'), 
                         ('src', os.path.join(image_dir, images[i]))]) \
               for i in range(5)]

        return render_template('recommendations.html', im_meta=im_meta)
    return

@app.route('/book/')
def onclick_book():
    book = request.form.getlist('book')
    # get information on the selected book and display the page
    return render_template('book.html')

if __name__ == '__main__':
    # Note from GCP Examples:
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    # Flask's development server will automatically serve static files in
    # the "static" directory. See:
    # http://flask.pocoo.org/docs/1.0/quickstart/#static-files. Once deployed,
    # App Engine itself will serve those files as configured in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
