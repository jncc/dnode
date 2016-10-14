
import { Component, Input, OnInit } from '@angular/core';
import { ActivatedRoute, Params } from '@angular/router';
import { Location } from '@angular/common';

import { Hero } from './hero';
import { HeroService } from './hero.service';

@Component({
  selector: 'my-hero-detail',
  moduleId: module.id, // http://schwarty.com/2015/12/22/angular2-relative-paths-for-templateurl-and-styleurls/
  templateUrl: 'hero-detail.component.html'
})
export class HeroDetailComponent implements OnInit {

  constructor(
    private heroService: HeroService,
    private route:       ActivatedRoute,
    private location:    Location
  ) { }

  ngOnInit() {
    // each time params (observable) changes...
    this.route.params.forEach((params: Params) => {

      let id: number = Number(params['id']);
      this.heroService.getHero(id).then(hero => this.hero = hero);
    });
  }

  goBack() {
    this.location.back();
  }

  @Input()
  hero: Hero
}
